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


static int64_t gc_count=0;


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




void gc_list_AddEnd(struct gc_list_type* gc_data)
{

    struct gc_list_type *node,*htemp;

    if (!(node=(struct gc_list_type *)malloc(sizeof(struct gc_list_type))))//分配空间
    {
        printf("fail alloc space!\n");	
        return NULL;
    }
    else
    {

    	node->gc_containerid=gc_data->gc_containerid;//保存数据
        node->gc_fp=gc_data->gc_fp;//保存数据
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
            	printf("1");
        	}

        	htemp->next=node;
        }

    }
}


void start_garbage_collection()
{
	gc_reference_count();
}

void gc_reference_count()
{
	int deleteversion;
	printf("please input the versition you want to delete!\n");
	scanf("%d",&deleteversion);

	

	gc_count=get_gc_reference_count(deleteversion);
	
	get_delete_message();

	printf("finish garbage collection\n");
}

int64_t get_gc_reference_count(int n)
{
	printf("gc in the gc_reference_count\n");
	
	return n;
}

void get_delete_message()
{
	int64_t size;
	size=gc_count*4/1024;
	printf("garbage collection finished\n");
	printf("the collection  size is %ld MB\n", size);
}


void do_gc(int revision, char *path) {

    init_recipe_store();

    init_gc_jcr(revision);

    //read rc struct from disk for gc
    read_rc_struct_from_disk();

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

    //pthread_create(&write_t, NULL, write_restore_data, NULL);

    do{
        sleep(5);
        /*time_t now = time(NULL);*/
        fprintf(stderr, "%" PRId64 " bytes, %" PRId32 " chunks, %d files processed\r", 
                jcr.data_size, jcr.chunk_num, jcr.file_num);
    }while(jcr.status == JCR_STATUS_RUNNING || jcr.status != JCR_STATUS_DONE);
    fprintf(stderr, "%" PRId64 " bytes, %" PRId32 " chunks, %d files processed\n", 
        jcr.data_size, jcr.chunk_num, jcr.file_num);

    assert(sync_queue_size(restore_chunk_queue) == 0);
    assert(sync_queue_size(restore_recipe_queue) == 0);

    free_backup_version(jcr.bv);

    TIMER_END(1, jcr.total_time);
    puts("==== gc end ====");

    printf("job id: %" PRId32 "\n", jcr.id);
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
            jcr.data_size * 1000000 / jcr.read_chunk_time / 1024 / 1024);
    /*printf("write_chunk_time : %.3fs, %.2fMB/s\n",
            jcr.write_chunk_time / 1000000,
            jcr.data_size * 1000000 / jcr.write_chunk_time / 1024 / 1024);
*/
    char logfile[] = "restore.log";
    FILE *fp = fopen(logfile, "a");

    /*
     * job id,
     * chunk num,
     * data size,
     * actually read container number,
     * speed factor,
     * throughput
     */
    fprintf(fp, "%" PRId32 " %" PRId64 " %" PRId32 " %.4f %.4f\n", jcr.id, jcr.data_size,
            jcr.read_container_num,
            jcr.data_size / (1024.0 * 1024 * jcr.read_container_num),
            jcr.data_size * 1000000 / (1024 * 1024 * jcr.total_time));

    fclose(fp);

    close_container_store();
    close_recipe_store();


}

static void* gc_read_recipe_thread(void *arg) {

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

            if (!g_hash_table_contains (rc_htable,cp->fp))
            {
                printf("error : fp can't find in rc_htable\n");
            }
            else if (g_hash_table_contains(rc_htable,&cp->fp))
            {
                struct rc_value * temp_rc=g_hash_table_lookup(rc_htable,&cp->fp);

                struct rc_value * temp2=(struct rc_value*)malloc(sizeof(struct rc_value));

                temp2->reference_count=temp_rc->reference_count-1;

                temp2->id=cp->id;

                if (temp2->reference_count==0)
                {
                    g_hash_table_remove(rc_htable,&cp->fp);

                    struct gc_list_type *node;

                    node=(struct gc_list_type *)malloc(sizeof(struct gc_list_type);

                    node->id=cp->id;
                    
                    node->fp=cp->fp;
                    gc_list_AddEnd(node);

                    gc_count++;
                }
                else
                {
                     g_hash_table_replace (rc_htable,&cp->fp,temp2);
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







