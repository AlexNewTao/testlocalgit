/*
author:  taoliu_alex

time: 2017.2.14

title :reference count

*/

#include <stdio.h>
#include <stdlib.h>
#include <./destor.h>


typedef unsigned char fingerprint[20];
typedef int64_t containerid; //container id

/*struct rc_list{
	containerid id;
    fingerprint fp;
	int reference_count;
	struct rc_list *next;
};


void add_to_rc(struct rc_list* rc_data)
{

    struct rc_list *node,*htemp;

    if (!(node=(struct rc_list *)malloc(sizeof(struct rc_list))))//åˆ†é…ç©ºé—´
    {
        printf("fail alloc space!\n");	
        return NULL;
    }
    else
    {

    	node->id=rc_data->id;//ä¿å­˜æ•°æ®
        node->fp=rc_data->fp;//ä¿å­˜æ•°æ®
        node->reference_count=rc_data->reference_count;
        node->next=NULL;//è®¾ç½®ç»“ç‚¹æŒ‡é’ˆä¸ºç©ºï¼Œå³ä¸ºè¡¨å°¾ï¼›

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
}*/

struct rc_value
{
	fingerprint fp;
	int reference_count;

};
 gboolean g_fingerprint_equal(fingerprint* fp1, fingerprint* fp2) {
	return !memcmp(fp1, fp2, sizeof(fingerprint));
}

static GHashTable *rc_htable;

//static long int fp_number=0;


void update_reference_count(struct segment *s)
{
    rc_htable=g_hash_table_new_full(g_int64_hash,fingerprint_equal, NULL, NULL);

    GSequenceIter *iter = g_sequence_get_begin_iter(s->chunks);
    GSequenceIter *end = g_sequence_get_end_iter(s->chunks);
    for (; iter != end; iter = g_sequence_iter_next(iter)) 
    {
        struct chunk* c = g_sequence_get(iter);

        if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
            continue;

        if (CHECK_CHUNK(c,CHUNK_UNIQUE))
        {
            struct rc_value * temp=(struct rc_value*)malloc(sizeof(struct rc_value));
            temp->reference_count=1;
            temp->id=c->id;
            g_hash_table_insert(rc_htable,&c->fp,temp);
        }
        if (CHECK_CHUNK(c,CHUNK_DUPLICATE))
        {
            if (!g_hash_table_contains (rc_htable,c->fp))
            {
                struct rc_value * temp1=(struct rc_value*)malloc(sizeof(struct rc_value));
                temp1->reference_count=1;
                temp1->id=c->id;
                g_hash_table_insert(rc_htable,&c->fp,temp1);
            }
            else if (g_hash_table_contains (rc_htable,c->fp))
            {
                struct rc_value * temp_rc=g_hash_table_lookup(rc_htable,&c->fp);

                struct rc_value * temp2=(struct rc_value*)malloc(sizeof(struct rc_value));

                temp2->reference_count=temp_rc->reference_count+1;

                temp2->id=c->id;

                g_hash_table_replace (rc_htable,&c->fp,temp2);
            }
            
        }

    }

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

    int keynum=g_hash_table_size(rc_htable);

    fwrite(&keynum, sizeof(int), 1, fp);

    GHashTableIter iter;

    gpointer key;

    gpointer value;

    g_hash_table_iter_init(&iter, rc_htable);

    while (g_hash_table_iter_next(&iter, &key, &value)) {

        /* Write a gc_feature. */
        //first write gc_feature fp

        if(fwrite(key, destor.index_key_size, 1, fp) != 1){
            perror("Fail to write a key!");
            exit(1);
        }

        //½á¹¹ÌåÒ»ÆðÐ´Èë´ÅÅÌ
        if(fwrite(value, sizeof(struct rc_value ), 1, fp) != 1){
            perror("Fail to write a value!");
            exit(1);
        }
        //·Ö¿ªÐ´Èë´ÅÅÌ
        //struct rc_value *rc_value1=value;
           
        //int id=rc_value1->id;
        //int reference_count=rc_value1->reference_count;
    /*    if(fwrite(&value->id, sizeof(int64_t ), 1, fp) != 1){
            perror("Fail to write a id!");
            exit(1);
        }

        if(fwrite(&value->reference_count, sizeof(int), 1, fp) != 1){
            perror("Fail to write a reference_count!");
            exit(1);
        }*/
    }

    fclose(fp);

    sdsfree(rc_path);

    g_hash_table_destroy(rc_htable);
}

void read_rc_struct_from_disk()
{
    rc_htable=g_hash_table_new_full(g_int64_hash,fingerprint_equal, NULL, NULL);
    
    sds rc_path = sdsdup(destor.working_directory);
    rc_path = sdscat(rc_path, "reference_count.rc");

    FILE *fp;
    if ((fp = fopen(rc_path, "r"))) {
        int keynum;
        fread(&keynum, sizeof(int), 1, fp);

        int i;
        for (; keynum>0; keynum--)
        {
            //read the gc_feature;
            int key;
            fread(&key, sizeof(fingerprint), 1, fp);

            int64_t id;
            int reference_count;

            fread(&id, sizeof(int64_t), 1, fp);
            fread(&reference_count, sizeof(int), 1, fp);

            struct rc_value * temp=(struct rc_value*)malloc(sizeof(struct rc_value));
            temp->reference_count=reference_count;
            temp->id=id;
            g_hash_table_insert(rc_htable,&key,temp);
        }
        fclose(fp);
    }
    
    sdsfree(rc_path);
}

