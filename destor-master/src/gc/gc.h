/*
author:  taoliu_alex

time: 2017.1.16

title :reference count

*/


#ifndef GC_H
#define GC_H


struct gc_list_type{
	int64_t gc_containerid;
	int32_t gc_chunk_shift;
	struct gc_list_type *next;
};

void Destory_gc_list();


void gc_list_AddEnd(struct gc_list_type* gc_data);

void start_garbage_collection();







#endif  