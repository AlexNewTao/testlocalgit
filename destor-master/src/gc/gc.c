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
#include "../jcr.h"
#include "../recipe/recipestore.h"
#include "../storage/containerstore.h"
#include "../utils/lru_cache.h"
#include "../restore.h"
#include "rc.h"

int64_t gc_count=0;
/*struct rc_list{
    containerid id;
    fingerprint fp;
    struct rc_list *next;
};*/

void Destory_rc_list()
{
    struct rc_list *p = gchead;
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

    TIMER_DECLARE(1);
    TIMER_BEGIN(1);

    int i, j, k;
    for (i = 0; i < jcr.bv->number_of_files; i++) {
      

        struct fileRecipeMeta *r = read_next_file_recipe_meta(jcr.bv);

        struct chunk *c = new_chunk(sdslen(r->filename) + 1);
        strcpy(c->data, r->filename);
        SET_CHUNK(c, CHUNK_FILE_START);

        sync_queue_push(restore_recipe_queue, c);

        for (j = 0; j < r->chunknum; j++) {

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

            sync_queue_push(restore_recipe_queue, c);

            free(cp);
        }

        c = new_chunk(0);
        SET_CHUNK(c, CHUNK_FILE_END);
        sync_queue_push(restore_recipe_queue, c);

        free_file_recipe_meta(r);
    }

    sync_queue_term(restore_recipe_queue);

    TIMER_END(1, jcr.gc_list_time);
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




/*void do_gc(int revision, char *path) {

    printf("start garbage collection\n");

    init_recipe_store();

    init_container_store();

    //init_gc_jcr(revision);
    init_restore_jcr(revision, path);
    //read rc struct from disk for gc
    read_rc_struct_from_disk();

    printf("read_rc_struct_from_disk successful\n");
    
    //struct backupVersion* bv = open_backup_version(revision);

    destor_log(DESTOR_NOTICE, "job id: %d", jcr.id);
    destor_log(DESTOR_NOTICE, "backup path: %s", jcr.bv->path);
    destor_log(DESTOR_NOTICE, "restore to: %s", jcr.path);

    restore_chunk_queue = sync_queue_new(100);
    restore_recipe_queue = sync_queue_new(100);

    TIMER_DECLARE(1);
    TIMER_BEGIN(1); 

    puts("==== gc begin ====");

    jcr.status = JCR_STATUS_RUNNING;
    pthread_t recipe_t, read_t, write_t;
    pthread_create(&recipe_t, NULL, gc_read_recipe_thread, NULL);

    printf("1111111\n");
    if (destor.restore_cache[0] == RESTORE_CACHE_LRU) {
        destor_log(DESTOR_NOTICE, "restore cache is LRU");
        pthread_create(&read_t, NULL, lru_restore_thread, NULL);
    } else if (destor.restore_cache[0] == RESTORE_CACHE_OPT) {
        destor_log(DESTOR_NOTICE, "restore cache is OPT");
        pthread_create(&read_t, NULL, optimal_restore_thread, NULL);
    } else if (destor.restore_cache[0] == RESTORE_CACHE_ASM) {
        destor_log(DESTOR_NOTICE, "restore cache is ASM");
        pthread_create(&read_t, NULL, assembly_restore_thread, NULL);
    } else {
        fprintf(stderr, "Invalid restore cache.\n");
        exit(1);
    }

    printf("22222222\n");*/
    //pthread_create(&write_t, NULL, write_restore_data, NULL);

  /*  do{
        sleep(5);
    
        fprintf(stderr, "%" PRId64 " bytes, %" PRId32 " chunks, %d files processed\r", 
                jcr.data_size, jcr.chunk_num, jcr.file_num);
    }while(jcr.status == JCR_STATUS_RUNNING || jcr.status != JCR_STATUS_DONE);
    fprintf(stderr, "%" PRId64 " bytes, %" PRId32 " chunks, %d files processed\n", 
        jcr.data_size, jcr.chunk_num, jcr.file_num);*/

/*    printf("4444444444\n");
    assert(sync_queue_size(restore_chunk_queue) == 0);
    assert(sync_queue_size(restore_recipe_queue) == 0);
    */
    //jcr.bv = open_backup_version(revision);
    
    //jcr.bv->deleted = 1;

    //update_backup_version(bv);
       
  /*  free_backup_version(jcr.bv);
    printf("3333333\n");
    TIMER_END(1, jcr.total_time);
    puts("==== gc end ====");*/

    /*printf("job id: %" PRId32 "\n", jcr.id);
    printf("restore path: %s\n", jcr.path);
    printf("number of files: %" PRId32 "\n", jcr.file_num);
    printf("number of chunks: %" PRId32"\n", jcr.chunk_num);
    printf("total size(B): %" PRId64 "\n", jcr.data_size);
    printf("total time(s): %.3f\n", jcr.total_time / 1000000);
    printf("throughput(MB/s): %.2f\n",
            jcr.data_size * 1000000 / (1024.0 * 1024 * jcr.total_time));
    printf("speed factor: %.2f\n",
            jcr.data_size / (1024.0 * 1024 * jcr.read_container_num));

    printf("read_recipe_time : %.3fs, %.2fMB/s\n",
            jcr.read_recipe_time / 1000000,
            jcr.data_size * 1000000 / jcr.read_recipe_time / 1024 / 1024);
    printf("read_chunk_time : %.3fs, %.2fMB/s\n", jcr.read_chunk_time / 1000000,
            jcr.data_size * 1000000 / jcr.read_chunk_time / 1024 / 1024);*/
    /*printf("write_chunk_time : %.3fs, %.2fMB/s\n",
            jcr.write_chunk_time / 1000000,
            jcr.data_size * 1000000 / jcr.write_chunk_time / 1024 / 1024);
*/
  /*  char logfile[] = "restore.log";
    FILE *fp = fopen(logfile, "a");*/

    /*
     * job id,
     * chunk num,
     * data size,
     * actually read container number,
     * speed factor,
     * throughput
     */
    /*fprintf(fp, "%" PRId32 " %" PRId64 " %" PRId32 " %.4f %.4f\n", jcr.id, jcr.data_size,
            jcr.read_container_num,
            jcr.data_size / (1024.0 * 1024 * jcr.read_container_num),
            jcr.data_size * 1000000 / (1024 * 1024 * jcr.total_time));*/

    //fclose(fp);

    //close_container_store();
/*
    close_recipe_store();

    write_rc_struct_to_disk();

    Destory_rc_list();

    get_delete_message();

}*/






