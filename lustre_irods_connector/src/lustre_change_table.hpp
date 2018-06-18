#ifndef LUSTRE_CHANGE_TABLE_HPP
#define LUSTRE_CHANGE_TABLE_HPP

#include "inout_structs.h"

#include "config.hpp"
#include <string>
#include <ctime>
#include <vector>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/filesystem.hpp>

#include "change_table.capnp.h"


struct change_descriptor {
    unsigned long long            cr_index;
    std::string                   fidstr;
    std::string                   parent_fidstr;
    std::string                   object_name;
    std::string                   lustre_path;     // the lustre_path can be ascertained by the parent_fid and object_name
                                                   // however, if a parent is moved after calculating the lustre_path, we 
                                                   // may have to look up the path using iRODS metadata
    ChangeDescriptor::EventTypeEnum last_event; 
    time_t                        timestamp;
    bool                          oper_complete;
    ChangeDescriptor::ObjectTypeEnum object_type;
    off_t                         file_size;
};

struct change_descriptor_seq_idx {};
struct change_descriptor_fidstr_idx {};
struct change_descriptor_oper_complete_idx {};

typedef boost::multi_index::multi_index_container<
  change_descriptor,
  boost::multi_index::indexed_by<
    boost::multi_index::ordered_unique<
      boost::multi_index::tag<change_descriptor_seq_idx>,
      boost::multi_index::member<
        change_descriptor, unsigned long long, &change_descriptor::cr_index
      >
    >,
    boost::multi_index::hashed_unique<
      boost::multi_index::tag<change_descriptor_fidstr_idx>,
      boost::multi_index::member<
        change_descriptor, std::string, &change_descriptor::fidstr
      >
    >,
    boost::multi_index::hashed_non_unique<
      boost::multi_index::tag<change_descriptor_oper_complete_idx>,
      boost::multi_index::member<
        change_descriptor, bool, &change_descriptor::oper_complete
      >
    >

  >
> change_map_t;


// This is only to faciliate writing the fidstr to the root directory 
int lustre_write_fidstr_to_root_dir(const std::string& lustre_root_path, const std::string& fidstr, change_map_t& change_map);

int lustre_close(unsigned long long cr_index, const std::string& lustre_root_path, const std::string& fidstr, const std::string& parent_fidstr,
                     const std::string& object_name, const std::string& lustre_path, change_map_t& change_map);
int lustre_mkdir(unsigned long long cr_index, const std::string& lustre_root_path, const std::string& fidstr, const std::string& parent_fidstr,
                     const std::string& object_name, const std::string& lustre_path, change_map_t& change_map);
int lustre_rmdir(unsigned long long cr_index, const std::string& lustre_root_path, const std::string& fidstr, const std::string& parent_fidstr,
                     const std::string& object_name, const std::string& lustre_path, change_map_t& change_map);
int lustre_unlink(unsigned long long cr_index, const std::string& lustre_root_path, const std::string& fidstr, const std::string& parent_fidstr,
                     const std::string& object_name, const std::string& lustre_path, change_map_t& change_map);
int lustre_rename(unsigned long long cr_index, const std::string& lustre_root_path, const std::string& fidstr, const std::string& parent_fidstr,
                     const std::string& object_name, const std::string& lustre_path, const std::string& old_lustre_path, 
                     change_map_t& change_map);
int lustre_create(unsigned long long cr_index, const std::string& lustre_root_path, const std::string& fidstr, const std::string& parent_fidstr,
                     const std::string& object_name, const std::string& lustre_path, change_map_t& change_map);
int lustre_mtime(unsigned long long cr_index, const std::string& lustre_root_path, const std::string& fidstr, const std::string& parent_fidstr,
                     const std::string& object_name, const std::string& lustre_path, change_map_t& change_map);
int lustre_trunc(unsigned long long cr_index, const std::string& lustre_root_path, const std::string& fidstr, const std::string& parent_fidstr,
                     const std::string& object_name, const std::string& lustre_path, change_map_t& change_map);


int remove_fidstr_from_table(const std::string& fidstr, change_map_t& change_map);


void lustre_print_change_table(const change_map_t& change_map);
bool entries_ready_to_process(change_map_t& change_map);
int serialize_change_map_to_sqlite(change_map_t& change_map);
int deserialize_change_map_from_sqlite(change_map_t& change_map);
int initiate_change_map_serialization_database();
int set_update_status_in_capnproto_buf(unsigned char*& buf, size_t& buflen, const std::string& new_status);
int get_update_status_from_capnproto_buf(unsigned char* buf, size_t buflen, std::string& update_status);
void add_entries_back_to_change_table(change_map_t& change_map, std::shared_ptr<change_map_t>& removed_entries);
int add_capnproto_buffer_back_to_change_table(unsigned char* buf, size_t buflen, change_map_t& change_map, std::set<std::string>& current_active_fidstr_list);
void remove_fidstr_from_active_list(unsigned char* buf, size_t buflen, std::set<std::string>& current_active_fidstr_list);
int write_change_table_to_capnproto_buf(const lustre_irods_connector_cfg_t *config_struct_ptr, void*& buf, size_t& buflen,
                                          change_map_t& change_map, std::set<std::string>& current_active_fidstr_list); 
int get_cr_index(unsigned long long& cr_index);
int write_cr_index_to_sqlite(unsigned long long cr_index);


#endif


