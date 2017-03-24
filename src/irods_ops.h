#ifndef __irods_ops_h
#define __irods_ops_h

#include "rodsType.h"

#ifdef __cplusplus
extern "C" {
#endif

// src_path_lustre below is the physical path on the OS

int instantiate_irods_connection();

void disconnect_irods_connection();

int add_avu(const char *src_path_lustre, const char *attr, const char *val, const char *unit, bool is_collection);

int register_file(const char *src_path_lustre, const char *irods_path, const char *resource_name); 

int make_collection(const char *src_path_lustre);

// Gets the irods object with matching AVU.
// This returns the first one found so the expectation is that the AVU combination is unique.
int find_irods_path_with_avu(const char *attr, const char *value, const char *unit, bool is_collection, char *irods_path);

int remove_data_object(const char *src_path_irods);

int remove_collection(const char *src_path_irods);

int rename_data_object(const char *src_path_irods, const char *dest_path_irods);

int rename_collection(const char *src_path_irods, const char *dest_path_irods);

int update_vault_path_for_data_object(const char *irods_path, const char *new_vault_path);

int get_irods_path_from_lustre_path(const char *lustre_path, char *irods_path);

int update_data_object_size(const char *irods_path, rodsLong_t size);

int update_data_object_modify_time(const char *irods_path, time_t modify_time); 


#ifdef __cplusplus
}
#endif

#endif
