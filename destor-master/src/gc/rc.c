/*
author:  taoliu_alex

time: 2017.2.14

title :reference count

*/

#include <stdio.h>
#include <stdlib.h>

#include "../destor.h"
#include "rc.h"
#include "../jcr.h"

typedef unsigned char fingerprint[20];

typedef int64_t containerid; //container id



gboolean fingerprint_equal(fingerprint* fp1, fingerprint* fp2) {
    return !memcmp(fp1, fp2, sizeof(fingerprint));
}


int unique_count_key=0;
int dedup_count_key=0;
int count_key=0;

void update_reference_count(struct segment *s)
{
    //rc_htable=g_hash_table_new_full(g_int64_hash,fingerprint_equal, NULL, NULL);

    GSequenceIter *iter = g_sequence_get_begin_iter(s->chunks);
    GSequenceIter *end = g_sequence_get_end_iter(s->chunks);
    for (; iter != end; iter = g_sequence_iter_next(iter)) 
    {
        struct chunk* c = g_sequence_get(iter);

        if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
        {
            continue;
        }
        else if (CHECK_CHUNK(c,CHUNK_DUPLICATE))
        {
            count_key++;
            dedup_count_key++;

            if (g_hash_table_contains(rc_htable,&c->fp))
            {
                
                struct rc_value * temp_rc=(struct rc_value*)malloc(sizeof(struct rc_value));

                temp_rc=g_hash_table_lookup(rc_htable,&c->fp);

                //struct rc_value * temp2=(struct rc_value*)malloc(sizeof(struct rc_value));

                temp_rc->reference_count=temp_rc->reference_count+1;
                //printf("%d",temp_rc->reference_count );

                //printf("%d",temp2->reference_count);
                //temp_rc->id=c->id;

                //g_hash_table_replace(rc_htable,&c->fp,temp_rc);
            }
            else
            {
                //printf("no fingerprint exist,worry!\n");
                //exit(1);
                /*struct rc_value * temp1=(struct rc_value*)malloc(sizeof(struct rc_value));
                temp1->reference_count=1;
                //printf("%d",temp1->reference_count );
                temp1->id=c->id;
                g_hash_table_insert(rc_htable,&c->fp,temp1);*/
            }

        }
        else//means the chunk is unique!
        {
            unique_count_key++;
            count_key++;
            if (!g_hash_table_contains (rc_htable,&c->fp))
            {
                struct rc_value * temp1=(struct rc_value*)malloc(sizeof(struct rc_value));
                temp1->reference_count=1;
                //printf("%d",temp1->reference_count );
                temp1->id=c->id;
                fingerprint *key = malloc(sizeof(fingerprint));
                memcpy(key,c->fp,sizeof(fingerprint));
                g_hash_table_insert(rc_htable,key,temp1);
                
            }
            else if(g_hash_table_contains (rc_htable,&c->fp))
            {

                /*struct rc_value * temp_rc=(struct rc_value*)malloc(sizeof(struct rc_value));

                temp_rc=g_hash_table_lookup(rc_htable,&c->fp);

                //struct rc_value * temp2=(struct rc_value*)malloc(sizeof(struct rc_value));

                temp_rc->reference_count=temp_rc->reference_count+1;

                    //printf("%d",temp2->reference_count);

                temp_rc->id=c->id;
                g_hash_table_replace(rc_htable,&c->fp,temp_rc);*/

                printf("the unique fp exist ! impossible!\n");
                exit(1);
            }
        }
        /*if (CHECK_CHUNK(c,CHUNK_UNIQUE))
        {
            uniuqe_count_key++;
            if (!g_hash_table_contains (rc_htable,&c->fp))
            {
                
                struct rc_value * temp1=(struct rc_value*)malloc(sizeof(struct rc_value));
                temp1->reference_count=1;
                //printf("%d",temp1->reference_count );
                temp1->id=c->id;
                g_hash_table_insert(rc_htable,&c->fp,temp1);
            }
            else if (g_hash_table_contains (rc_htable,&c->fp))
            {

                struct rc_value * temp_rc=(struct rc_value*)malloc(sizeof(struct rc_value));

                temp_rc=g_hash_table_lookup(rc_htable,&c->fp);

                //struct rc_value * temp2=(struct rc_value*)malloc(sizeof(struct rc_value));

                temp_rc->reference_count=temp_rc->reference_count+1;

                //printf("%d",temp2->reference_count);

                temp_rc->id=c->id;

                g_hash_table_replace(rc_htable,&c->fp,temp_rc);
            }
        }
        else if (CHECK_CHUNK(c,CHUNK_DUPLICATE)||CHECK_CHUNK(c, CHUNK_REWRITE_DENIED))
        {
            dedup_count_key++;
            if (!g_hash_table_contains (rc_htable,&c->fp))
            {

                struct rc_value * temp1=(struct rc_value*)malloc(sizeof(struct rc_value));
                temp1->reference_count=1;
                //printf("%d",temp1->reference_count );
                temp1->id=c->id;
                g_hash_table_insert(rc_htable,&c->fp,temp1);
            }
            else if (g_hash_table_contains (rc_htable,&c->fp))
            {

                struct rc_value * temp_rc=(struct rc_value*)malloc(sizeof(struct rc_value));

                temp_rc=g_hash_table_lookup(rc_htable,&c->fp);

                temp_rc->reference_count=temp_rc->reference_count+1;

                //printf("%d",temp2->reference_count);

                temp_rc->id=c->id;

                g_hash_table_replace(rc_htable,&c->fp,temp_rc);
            }
            
        }
        else
            extra_count_key++;*/
    }
    //printf("update_reference_count 111111111\n");
}



void write_rc_struct_to_disk()
{
    

    sds rc_path = sdsdup(destor.working_directory);
    rc_path = sdscat(rc_path, "reference_count.rc");

    FILE *fp;
    if ((fp = fopen(rc_path, "w")) == NULL) {
        perror("Can not open reference_count.rc for write because:");
        exit(1);
    }
    
    TIMER_DECLARE(1);
    TIMER_BEGIN(1);

    int64_t keynum=g_hash_table_size(rc_htable);

    printf("the key number is %d\n",keynum);

    fwrite(&keynum, sizeof(int64_t), 1, fp);

    GHashTableIter iter;

    gpointer key;

    gpointer value;

    g_hash_table_iter_init(&iter, rc_htable);

    while (g_hash_table_iter_next(&iter, &key, &value)) {

        /* Write a gc_feature. */
        //first write gc_feature fp

        if(fwrite(key, sizeof(fingerprint), 1, fp) != 1){
            perror("Fail to write a key!");
            exit(1);
        }

        //结构体一起写入磁盘
        /*if(fwrite(value, sizeof(struct rc_value ), 1, fp) != 1){
            perror("Fail to write a value!");
            exit(1);
        }*/
        //分开写入磁盘
        struct rc_value *rc_value1=(struct rc_value *)value;
           
        int id=rc_value1->id;
        int reference_count=rc_value1->reference_count;
        if(fwrite(&id, sizeof(int64_t ), 1, fp) != 1){
            perror("Fail to write a id!");
            exit(1);
        }

        if(fwrite(&reference_count, sizeof(int), 1, fp) != 1){
            perror("Fail to write a reference_count!");
            exit(1);
        }


    }

    fclose(fp);

    sdsfree(rc_path);

    g_hash_table_destroy(rc_htable);

    TIMER_END(1, jcr.write_rc_struct_time);

    printf("write_rc_struct_to_disk successful\n");

    printf("the unique count key is %d\n",unique_count_key );
    printf("the dedup count key is %d\n",dedup_count_key );
    //printf("the extra count key is %d\n",extra_count_key );
    printf("the count key is %d\n",count_key );

}

void init_rc_struct(int n)
{
    if (n==0)
    {
        rc_htable=g_hash_table_new_full(g_int64_hash,fingerprint_equal, NULL, NULL); 
        printf("aaaaaaaaa\n");
    }
    else
    {
        read_rc_struct_from_disk();
        printf("bbbbbbbb\n");
    }
    printf("init_rc_struct successful\n");
}


int64_t read_key=0;

void read_rc_struct_from_disk()
{
    TIMER_DECLARE(1);
    TIMER_BEGIN(1);

    rc_htable=g_hash_table_new_full(g_int64_hash,fingerprint_equal, NULL, NULL);
    
    sds rc_path = sdsdup(destor.working_directory);
    rc_path = sdscat(rc_path, "reference_count.rc");

    printf("first the sizeof key read is %ld\n",g_hash_table_size(rc_htable));

    FILE *fp;
    if ((fp = fopen(rc_path, "r"))) {
        int64_t keynum;
        fread(&keynum, sizeof(int64_t), 1, fp);

        printf("the keynum read is %ld\n",keynum);

        for (; keynum>0; keynum--)
        {
            //read the gc_feature;
            fingerprint *key = malloc(sizeof(fingerprint));
            
            fread(key, sizeof(fingerprint), 1, fp);

            int64_t id;
            int reference_count;

            fread(&id, sizeof(int64_t), 1, fp);
            fread(&reference_count, sizeof(int), 1, fp);

            struct rc_value * temp=(struct rc_value*)malloc(sizeof(struct rc_value));
            temp->reference_count=reference_count;
            temp->id=id;
            
            g_hash_table_insert(rc_htable,key,temp);

            read_key++;
        }
        fclose(fp);
    }

    
    sdsfree(rc_path);
    printf("the read key is %ld\n",read_key);

    TIMER_END(1, jcr.read_rc_struct_time);
    printf("last the sizeof key read is %ld\n",g_hash_table_size(rc_htable));
}