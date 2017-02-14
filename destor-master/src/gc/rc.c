/*
author:  taoliu_alex

time: 2017.2.14

title :reference count

*/

#include <stdio.h>
#include <stdlib.h>


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

    if (!(node=(struct rc_list *)malloc(sizeof(struct rc_list))))//分配空间
    {
        printf("fail alloc space!\n");	
        return NULL;
    }
    else
    {

    	node->id=rc_data->id;//保存数据
        node->fp=rc_data->fp;//保存数据
        node->reference_count=rc_data->reference_count;
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
}*/

struct rc_value
{
	fingerprint fp;
	int reference_count;

};

static gboolean g_fingerprint_equal(fingerprint* fp1, fingerprint* fp2) {
	return !memcmp(fp1, fp2, sizeof(fingerprint));
}

static GHashTable *rc_htable;

static long int fp_number=0;

void update_reference_count(struct segment *s)
{
	rc_htable=g_hash_table_new_full(g_int64_hash,g_fingerprint_equal, NULL, NULL);

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
				g_hash_table_insert(rc_htable,&c->fp,temp);
			}
			else if (g_hash_table_contains (rc_htable,c->fp))
			{
				int temp_rc=g_hash_table_lookup (rc_htable,c->fp);

				struct rc_value * temp2=(struct rc_value*)malloc(sizeof(struct rc_value));

				temp2->reference_count=temp_rc->reference_count+1;

				temp2->id=c->id;

				g_hash_table_replace (rc_htable,&c->fp,temp2);
			}
			
		}

	}

	write_rc_struct_to_disk();

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
}


void read_rc_struct_from_disk()
{
	sds rc_path = sdsdup(destor.working_directory);
	rc_path = sdscat(rc_path, "reference_count.rc");

	FILE *fp;
	if ((fp = fopen(rc_path, "r"))) {
	}

}