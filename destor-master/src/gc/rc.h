/*
author:  taoliu_alex

time: 2017.2.14

title :reference count

*/

#ifndef RC_H
#define RC_H

gboolean g_fingerprint_equal(fingerprint* fp1, fingerprint* fp2);



static GHashTable *rc_htable;

static long int fp_number=0;

void update_reference_count(struct segment *s)


void write_rc_struct_to_disk();

void read_rc_struct_from_disk();


#endif









