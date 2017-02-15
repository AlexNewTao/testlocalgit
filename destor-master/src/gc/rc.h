/*
author:  taoliu_alex

time: 2017.2.14

title :reference count

*/

#ifndef RC_H
#define RC_H

gboolean g_fingerprint_equal(fingerprint* fp1, fingerprint* fp2);




gboolean fingerprint_equal(fingerprint* fp1, fingerprint* fp2);

void update_reference_count(struct segment *s);

//void get_rc_struct();

void write_rc_struct_to_disk();

void read_rc_struct_from_disk();



#endif









