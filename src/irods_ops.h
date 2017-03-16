#ifndef __irods_ops_h
#define __irods_ops_h

#ifdef __cplusplus
extern "C" {
#endif

// src_path below is the physical path on the OS

int instantiate_irods_connection();

void disconnect_irods_connection();

int add_avu(const char *src_path, const char *attr, const char *val, const char *unit, bool is_collection);

int register_file(const char *src_path); 

int make_collection(const char *src_path);

// Gets the irods object with matching AVU.
// This returns the first one found so the expectation is that the AVU combination is unique.
int find_irods_path_with_avu(const char *attr, const char *value, const char *unit, bool is_collection, char *irods_path);

int remove_data_object(const char *src_path);

int remove_collection(const char *src_path);

#ifdef __cplusplus
}
#endif

#endif
