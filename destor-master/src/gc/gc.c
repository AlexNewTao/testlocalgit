/*
author:  taoliu_alex

time: 2017.1.16

title :reference count

*/

#include <stdio.h>
#include <stdlib.h>
#include <glib.h>
#include "gc.h"
#include "../destor.h"


int64_t gc_count=0;


void Destory_gc_list()
{
    struct gc_list_type *p = gchead;
    while(gchead!=NULL)
    {
         p = gchead;
         free(p);
         gchead = gchead->next;
    }
}




void add_to_rc(struct rc_list* rc_data)
{

    struct rc_list *node,*htemp;

    if (!(node=(struct rc_list *)malloc(sizeof(struct rc_list))))//分配空间
    {
        printf("fail alloc space!\n");  
    }
    else
    {

        node->id=rc_data->id;//保存数据

        //fingerprint *ft = malloc(sizeof(fingerprint));

        memcpy(&node->fp, &rc_data->fp, sizeof(fingerprint));
        
        //node->fp=ft;//保存数据
        
        node->next=NULL;//设置结点指针为空，即为表尾；

        if (gchead==NULL)

        {
            gchead=node;
          
        }
        else
        {
            htemp=gchead;
    
            while(htemp->next!=NULL)
            {
                htemp=htemp->next;
               
            }

            htemp->next=node;
        }

    }
}





void get_delete_message()
{
    int64_t size;
    printf("the  gc_count is %ld\n",gc_count);
    size=gc_count*4/1024;
    printf("garbage collection finished\n");
    printf("the collection  size is %ld MB\n", size);
}



void* gc_read_recipe_thread(void *arg) {

    int i, j, k;
    for (i = 0; i < jcr.bv->number_of_files; i++) {
        TIMER_DECLARE(1);
        TIMER_BEGIN(1);

        struct fileRecipeMeta *r = read_next_file_recipe_meta(jcr.bv);

        struct chunk *c = new_chunk(sdslen(r->filename) + 1);
        strcpy(c->data, r->filename);
        SET_CHUNK(c, CHUNK_FILE_START);

        TIMER_END(1, jcr.read_recipe_time);

        sync_queue_push(restore_recipe_queue, c);

        for (j = 0; j < r->chunknum; j++) {

            TIMER_DECLARE(1);
            TIMER_BEGIN(1);

            struct chunkPointer* cp = read_next_n_chunk_pointers(jcr.bv, 1, &k);

            if (!g_hash_table_contains (rc_htable,&cp->fp))
            {
                printf("error : fp can't find in rc_htable\n");
                exit(1);
            }
            else if (g_hash_table_contains(rc_htable,&cp->fp))
            {
                struct rc_value* temp_rc=g_hash_table_lookup(rc_htable,&cp->fp);

                //struct rc_value* temp2=(struct rc_value*)malloc(sizeof(struct rc_value));

                temp_rc->reference_count=temp_rc->reference_count-1;

                //temp_rc->id=cp->id;
                if (temp_rc->reference_count==0)
                {

                    struct rc_list *newnode;

                    newnode = (struct rc_list *)malloc(sizeof(struct rc_list));

                    newnode->id=cp->id;

                    //fingerprint *key = malloc(sizeof(fingerprint));
                    
                    //memcpy(key,&cp->fp,sizeof(fingerprint));

                    //fingerprint *ft = malloc(sizeof(fingerprint));

                    memcpy(&newnode->fp, &cp->fp, sizeof(fingerprint));

                    add_to_rc(newnode);

                    g_hash_table_remove(rc_htable,&cp->fp);

                    gc_count++;
                }
               
            }

            struct chunk* c = new_chunk(0);
            memcpy(&c->fp, &cp->fp, sizeof(fingerprint));
            c->size = cp->size;
            c->id = cp->id;

            TIMER_END(1, jcr.read_recipe_time);

            sync_queue_push(restore_recipe_queue, c);


            free(cp);
        }

        c = new_chunk(0);
        SET_CHUNK(c, CHUNK_FILE_END);
        sync_queue_push(restore_recipe_queue, c);

        free_file_recipe_meta(r);
    }

    sync_queue_term(restore_recipe_queue);
    return NULL;
}

static void* lru_restore_thread(void *arg) {
    struct lruCache *cache;
    if (destor.simulation_level >= SIMULATION_RESTORE)
        cache = new_lru_cache(destor.restore_cache[1], free_container_meta,
                lookup_fingerprint_in_container_meta);
    else
        cache = new_lru_cache(destor.restore_cache[1], free_container,
                lookup_fingerprint_in_container);

    struct chunk* c;
    while ((c = sync_queue_pop(restore_recipe_queue))) {

        if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
            sync_queue_push(restore_chunk_queue, c);
            continue;
        }

        TIMER_DECLARE(1);
        TIMER_BEGIN(1);

        if (destor.simulation_level >= SIMULATION_RESTORE) {
            struct containerMeta *cm = lru_cache_lookup(cache, &c->fp);
            if (!cm) {
                VERBOSE("Restore cache: container %lld is missed", c->id);
                cm = retrieve_container_meta_by_id(c->id);
                assert(lookup_fingerprint_in_container_meta(cm, &c->fp));
                lru_cache_insert(cache, cm, NULL, NULL);
                jcr.read_container_num++;
            }

            TIMER_END(1, jcr.read_chunk_time);
        } else {
            struct container *con = lru_cache_lookup(cache, &c->fp);
            if (!con) {
                VERBOSE("Restore cache: container %lld is missed", c->id);
                con = retrieve_container_by_id(c->id);
                lru_cache_insert(cache, con, NULL, NULL);
                jcr.read_container_num++;
            }
            struct chunk *rc = get_chunk_in_container(con, &c->fp);
            assert(rc);
            TIMER_END(1, jcr.read_chunk_time);
            sync_queue_push(restore_chunk_queue, rc);
        }

        jcr.data_size += c->size;
        jcr.chunk_num++;
        free_chunk(c);
    }

    sync_queue_term(restore_chunk_queue);

    free_lru_cache(cache);

    return NULL;
}






