/*
author:  taoliu_alex

time: 2017.1.16

title :reference count

*/


#ifndef GC_H
#define GC_H

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

struct rc_list{
    int64_t id;
    fingerprint fp;
    struct rc_list *next;
};

int64_t gc_count;

struct rc_list *gchead;

void Destory_gc_list();

void add_to_rc(struct rc_list* rc_data);

void* gc_read_recipe_thread(void *arg);

//void do_gc(int revision);
void do_gc(int revision, char *path);
void get_delete_message();




#endif  