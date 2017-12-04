#ifndef __LUSTRE_CHANGE_TABLE_HPP
#define __LUSTRE_CHANGE_TABLE_HPP

#include "inout_structs.h"

#include "config.hpp"
#include <string>
#include <ctime>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/filesystem.hpp>

#include "change_table.capnp.h"

#include <string>

struct change_descriptor {
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
    boost::multi_index::sequenced<
      boost::multi_index::tag<change_descriptor_seq_idx>
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


int lustre_close(const std::string& lustre_root_path, const std::string& fidstr, const std::string& parent_fidstr,
                     const std::string& object_name, const std::string& lustre_path, change_map_t& change_map);
int lustre_mkdir(const std::string& lustre_root_path, const std::string& fidstr, const std::string& parent_fidstr,
                     const std::string& object_name, const std::string& lustre_path, change_map_t& change_map);
int lustre_rmdir(const std::string& lustre_root_path, const std::string& fidstr, const std::string& parent_fidstr,
                     const std::string& object_name, const std::string& lustre_path, change_map_t& change_map);
int lustre_unlink(const std::string& lustre_root_path, const std::string& fidstr, const std::string& parent_fidstr,
                     const std::string& object_name, const std::string& lustre_path, change_map_t& change_map);
int lustre_rename(const std::string& lustre_root_path, const std::string& fidstr, const std::string& parent_fidstr,
                     const std::string& object_name, const std::string& lustre_path, const std::string& old_lustre_path, 
                     change_map_t& change_map);
int lustre_create(const std::string& lustre_root_path, const std::string& fidstr, const std::string& parent_fidstr,
                     const std::string& object_name, const std::string& lustre_path, change_map_t& change_map);
int lustre_mtime(const std::string& lustre_root_path, const std::string& fidstr, const std::string& parent_fidstr,
                     const std::string& object_name, const std::string& lustre_path, change_map_t& change_map);
int lustre_trunc(const std::string& lustre_root_path, const std::string& fidstr, const std::string& parent_fidstr,
                     const std::string& object_name, const std::string& lustre_path, change_map_t& change_map);


int remove_fidstr_from_table(const std::string& fidstr, change_map_t& change_map);


void lustre_print_change_table(const change_map_t& change_map);
void lustre_write_change_table_to_str(char *buffer, const size_t buffer_size, const change_map_t& change_map);
bool entries_ready_to_process(change_map_t& change_map);
int serialize_change_map_to_sqlite(change_map_t& change_map);
int deserialize_change_map_from_sqlite(change_map_t& change_map);
int initiate_change_map_serialization_database();
void add_entries_back_to_change_table(change_map_t& change_map, std::shared_ptr<change_map_t>& removed_entries);
int add_capnproto_buffer_back_to_change_table(unsigned char* buf, int buflen, change_map_t& change_map);
int write_change_table_to_capnproto_buf(const lustre_irods_connector_cfg_t *config_struct_ptr, void*& buf, int& buflen,
    change_map_t& change_map);

#endif


