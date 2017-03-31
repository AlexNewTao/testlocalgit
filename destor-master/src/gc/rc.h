/*
author:  taoliu_alex

time: 2017.2.14

title :reference count

*/

#ifndef RC_H
#define RC_H

GHashTable *rc_htable;

struct rc_value
{
    int64_t id;
    int reference_count;
};

gboolean fingerprint_equal(fingerprint* fp1, fingerprint* fp2);

void update_reference_count(struct segment *s);

void init_rc_struct(int n);

//void get_rc_struct();

void write_rc_struct_to_disk();

void read_rc_struct_from_disk();

#endif









