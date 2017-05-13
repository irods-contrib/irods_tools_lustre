#ifndef __LUSTRE_CHANGE_TABLE_HPP
#define __LUSTRE_CHANGE_TABLE_HPP

#include "../../irods_lustre_api/src/inout_structs.h"

#ifdef __cplusplus
#include "change_table.capnp.h"
extern "C" {
#endif

void lustre_close(const char *fidstr, const char *parent_fidstr, const char *object_name, const char *lustre_path);
void lustre_mkdir(const char *fidstr, const char *parent_fidstr, const char *object_name, const char *lustre_path);
void lustre_rmdir(const char *fidstr, const char *parent_fidstr, const char *object_name, const char *lustre_path);
void lustre_unlink(const char *fidstr, const char *parent_fidstr, const char *object_name, const char *lustre_path);
void lustre_rename(const char *fidstr, const char *parent_fidstr, const char *object_name, const char *lustre_path, const char *old_lustre_path);
void lustre_create(const char *fidstr, const char *parent_fidstr, const char *object_name, const char *lustre_path);
void lustre_mtime(const char *fidstr, const char *parent_fidstr, const char *object_name, const char *lustre_path);
void lustre_trunc(const char *fidstr, const char *parent_fidstr, const char *object_name, const char *lustre_path);
void remove_fidstr_from_table(const char *fidstr);
void lustre_print_change_table();
void lustre_write_change_table_to_str(char *buffer, const size_t buffer_size);
void write_change_table_to_capnproto_buf(irodsLustreApiInp_t *inp);
bool entries_ready_to_process();


#ifdef __cplusplus
}
#endif


#ifdef __cplusplus

#include <string>
#include <ctime>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/hashed_index.hpp>


//enum create_delete_event_type_enum { OTHER, CREAT, UNLINK, RMDIR, MKDIR, RENAME };
//enum object_type_enum { _FILE, _DIR };
//

struct change_descriptor {
    std::string                   fidstr;
    std::string                   parent_fidstr;
    std::string                   object_name;
    std::string                   lustre_path;     // the lustre_path can be ascertained by the parent_fid and object_name
                                                   // however, if a parent is moved after calculating the lustre_path, we 
                                                   // may have to look up the path using iRODS metadata
    ChangeDescriptor::EventTypeEnum last_event; 
    //create_delete_event_type_enum last_event;
    time_t                        timestamp;
    bool                          oper_complete;
    ChangeDescriptor::ObjectTypeEnum object_type;
    //object_type_enum              object_type;
    off_t                         file_size;
};

typedef boost::multi_index::multi_index_container<
  change_descriptor,
  boost::multi_index::indexed_by<
    boost::multi_index::sequenced<>,
    boost::multi_index::hashed_unique<
      boost::multi_index::member<
        change_descriptor, std::string, &change_descriptor::fidstr
      >
    >,
    boost::multi_index::hashed_non_unique<
      boost::multi_index::member<
        change_descriptor, bool, &change_descriptor::oper_complete
      >
    >

  >
> change_map_t;

change_map_t *get_change_map_instance();

#endif

#endif

