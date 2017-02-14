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
        node->gc_chunk_shift=gc_data->gc_chunk_shift;//保存数据
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








