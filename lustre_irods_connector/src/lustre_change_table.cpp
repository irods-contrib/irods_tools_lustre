#include <string>
#include <ctime>
#include <sstream>
#include <mutex>
#include <thread>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>


// boost headers
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>

#include "lustre_change_table.hpp"
#include "inout_structs.h"
#include "logging.hpp"
#include "config.hpp"
#include "lustre_irods_errors.hpp"

// capnproto
#include "change_table.capnp.h"
#include <capnp/message.h>
#include <capnp/serialize-packed.h>

// sqlite
#include <sqlite3.h>

std::string event_type_to_str(ChangeDescriptor::EventTypeEnum type);
std::string object_type_to_str(ChangeDescriptor::ObjectTypeEnum type);


//using namespace boost::interprocess;

//static boost::shared_mutex change_table_mutex;
static std::mutex change_table_mutex;


int validate_operands(const char *lustre_root_path, const char *fidstr_cstr,
            const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, void *change_map_void_ptr) {

    if (lustre_root_path == nullptr) {
        LOG(LOG_ERR, "Null lustre_root_path sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    if (fidstr_cstr == nullptr) {
        LOG(LOG_ERR, "Null fidstr_cstr sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    if (parent_fidstr == nullptr) {
        LOG(LOG_ERR, "Null parent_fidstr sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    if (object_name == nullptr) {
        LOG(LOG_ERR, "Null object_name sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    if (lustre_path_cstr == nullptr) {
        LOG(LOG_ERR, "Null lustre_path_cstr sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    if (change_map_void_ptr == nullptr) {
        LOG(LOG_ERR, "Null change_map_void_ptr sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    return lustre_irods::SUCCESS;

}


extern "C" {

int lustre_close(const char *lustre_root_path, const char *fidstr_cstr, 
        const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, void *change_map_void_ptr) {

    int rc;
    if ((rc = validate_operands(lustre_root_path, fidstr_cstr, parent_fidstr, object_name, lustre_path_cstr,change_map_void_ptr)) < 0) {
        return rc;
    }

    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr);

    std::lock_guard<std::mutex> lock(change_table_mutex);
 
    // get change map with hashed index of fidstr
    auto &change_map_fidstr = change_map->get<change_descriptor_fidstr_idx>();
    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    struct stat st;
    int result = stat(lustre_path.c_str(), &st);

    LOG(LOG_DBG, "stat(%s, &st)\n", lustre_path.c_str());
    LOG(LOG_DBG, "handle_close:  stat_result = %i, file_size = %ld\n", result, st.st_size);

    auto iter = change_map_fidstr.find(fidstr);
    if (iter != change_map_fidstr.end()) {
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.oper_complete = true; });
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
        if (result == 0) {
            change_map_fidstr.modify(iter, [st](change_descriptor &cd){ cd.file_size = st.st_size; });
        }
    } else {
        // this is probably an append so no file update is done
        change_descriptor entry{};
        entry.fidstr = fidstr;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        entry.object_type = (result == 0 && S_ISDIR(st.st_mode)) ? ChangeDescriptor::ObjectTypeEnum::DIR : ChangeDescriptor::ObjectTypeEnum::FILE;
        entry.lustre_path = lustre_path; 
        entry.oper_complete = true;
        entry.timestamp = time(NULL);
        entry.last_event = ChangeDescriptor::EventTypeEnum::OTHER;
        if (result == 0) {
            entry.file_size = st.st_size;
        }

        change_map->push_back(entry);
    }
    return lustre_irods::SUCCESS;

}

int lustre_mkdir(const char *lustre_root_path, const char *fidstr_cstr, 
        const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, void *change_map_void_ptr) {

    int rc;
    if ((rc = validate_operands(lustre_root_path, fidstr_cstr, parent_fidstr, object_name, lustre_path_cstr,change_map_void_ptr)) < 0) {
        return rc;
    }

    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr);

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map_fidstr = change_map->get<change_descriptor_fidstr_idx>();
    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map_fidstr.find(fidstr);
    if(iter != change_map_fidstr.end()) {
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.oper_complete = true; });
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.last_event = ChangeDescriptor::EventTypeEnum::MKDIR; });
    } else {
        change_descriptor entry{};
        entry.fidstr = fidstr;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        entry.lustre_path = lustre_path;
        entry.oper_complete = true;
        entry.last_event = ChangeDescriptor::EventTypeEnum::MKDIR;
        entry.timestamp = time(NULL);
        entry.object_type = ChangeDescriptor::ObjectTypeEnum::DIR;
        change_map->push_back(entry);
    }
    return lustre_irods::SUCCESS; 

}

int lustre_rmdir(const char *lustre_root_path, const char *fidstr_cstr, 
        const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, void *change_map_void_ptr) {

    int rc;
    if ((rc = validate_operands(lustre_root_path, fidstr_cstr, parent_fidstr, object_name, lustre_path_cstr,change_map_void_ptr)) < 0) {
        return rc;
    }

   change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr);

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map_fidstr = change_map->get<change_descriptor_fidstr_idx>();

    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map_fidstr.find(fidstr);
    if(iter != change_map_fidstr.end()) {
        change_map_fidstr.modify(iter, [parent_fidstr](change_descriptor &cd){ cd.parent_fidstr = parent_fidstr; });
        change_map_fidstr.modify(iter, [object_name](change_descriptor &cd){ cd.object_name = object_name; });
        change_map_fidstr.modify(iter, [lustre_path](change_descriptor &cd){ cd.lustre_path = lustre_path; });
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.oper_complete = true; });
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.last_event = ChangeDescriptor::EventTypeEnum::RMDIR; });
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
    } else {
        change_descriptor entry{};
        entry.fidstr = fidstr;
        entry.oper_complete = true;
        entry.last_event = ChangeDescriptor::EventTypeEnum::RMDIR;
        entry.timestamp = time(NULL);
        entry.object_type = ChangeDescriptor::ObjectTypeEnum::DIR;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        change_map->push_back(entry);
    }
    return lustre_irods::SUCCESS; 


}

int lustre_unlink(const char *lustre_root_path, const char *fidstr_cstr, 
        const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, void *change_map_void_ptr) {
  
    int rc;
    if ((rc = validate_operands(lustre_root_path, fidstr_cstr, parent_fidstr, object_name, lustre_path_cstr,change_map_void_ptr)) < 0) {
        return rc;
    }

    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr);

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map_fidstr = change_map->get<change_descriptor_fidstr_idx>();

    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map_fidstr.find(fidstr);
    if(iter != change_map_fidstr.end()) {   

        // If an add and a delete occur in the same transactional unit, just delete the transaction
        if (iter->last_event == ChangeDescriptor::EventTypeEnum::CREATE) {
            change_map_fidstr.erase(iter);
        } else {
            change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.oper_complete = true; });
            change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.last_event = ChangeDescriptor::EventTypeEnum::UNLINK; });
            change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
       }
    } else {
        change_descriptor entry{};
        entry.fidstr = fidstr;
        //entry.parent_fidstr = parent_fidstr;
        //entry.lustre_path = lustre_path;
        entry.oper_complete = true;
        entry.last_event = ChangeDescriptor::EventTypeEnum::UNLINK;
        entry.timestamp = time(NULL);
        entry.object_type = ChangeDescriptor::ObjectTypeEnum::FILE;
        entry.object_name = object_name;
        change_map->push_back(entry);
    }

    return lustre_irods::SUCCESS; 
}

int lustre_rename(const char *lustre_root_path, const char *fidstr_cstr, 
        const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, 
        const char *old_lustre_path_cstr, void *change_map_void_ptr) {

    int rc;
    if ((rc = validate_operands(lustre_root_path, fidstr_cstr, parent_fidstr, object_name, lustre_path_cstr,change_map_void_ptr)) < 0) {
        return rc;
    }
    
    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr);

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map_fidstr = change_map->get<change_descriptor_fidstr_idx>();

    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);
    std::string old_lustre_path(old_lustre_path_cstr);

    auto iter = change_map_fidstr.find(fidstr);
    std::string original_path;

    struct stat statbuf;
    bool is_dir = stat(lustre_path.c_str(), &statbuf) == 0 && S_ISDIR(statbuf.st_mode);

    // if there is a previous entry, just update the lustre_path to the new path
    // otherwise, add a new entry
    if(iter != change_map_fidstr.end()) {
        change_map_fidstr.modify(iter, [parent_fidstr](change_descriptor &cd){ cd.parent_fidstr = parent_fidstr; });
        change_map_fidstr.modify(iter, [object_name](change_descriptor &cd){ cd.object_name = object_name; });
        change_map_fidstr.modify(iter, [lustre_path](change_descriptor &cd){ cd.lustre_path = lustre_path; });
    } else {
        change_descriptor entry{};
        entry.fidstr = fidstr;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        entry.lustre_path = lustre_path;
        entry.oper_complete = true;
        entry.last_event = ChangeDescriptor::EventTypeEnum::RENAME;
        entry.timestamp = time(NULL);
        if (is_dir) {
            entry.object_type = ChangeDescriptor::ObjectTypeEnum::DIR;
        } else  {
            entry.object_type = ChangeDescriptor::ObjectTypeEnum::FILE;
        }
        /*if (is_dir) {
            change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.object_type = ChangeDescriptor::ObjectTypeEnum::DIR; });
        } else {
            change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.object_type = ChangeDescriptor::ObjectTypeEnum::FILE; });
        }*/
        change_map->push_back(entry);
    }

    LOG(LOG_DBG, "rename:  old_lustre_path = %s\n", old_lustre_path.c_str());

    if (is_dir) {

        // search through and update all references in table
        for (auto iter = change_map_fidstr.begin(); iter != change_map_fidstr.end(); ++iter) {
            std::string p = iter->lustre_path;
            if (p.length() > 0 && p.length() != old_lustre_path.length() && boost::starts_with(p, old_lustre_path)) {
                //TODO test
                change_map_fidstr.modify(iter, [old_lustre_path, lustre_path](change_descriptor &cd){ cd.lustre_path.replace(0, old_lustre_path.length(), lustre_path); });
                //iter->lustre_path.replace(0, old_lustre_path.length(), lustre_path);
            }
        }
    }
    return lustre_irods::SUCCESS; 


}

int lustre_create(const char *lustre_root_path, const char *fidstr_cstr, 
        const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, void *change_map_void_ptr) {

    int rc;
    if ((rc = validate_operands(lustre_root_path, fidstr_cstr, parent_fidstr, object_name, lustre_path_cstr,change_map_void_ptr)) < 0) {
        return rc;
    }

    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr);

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map_fidstr = change_map->get<change_descriptor_fidstr_idx>();

    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map_fidstr.find(fidstr);
    if(iter != change_map_fidstr.end()) {
        change_map_fidstr.modify(iter, [parent_fidstr](change_descriptor &cd){ cd.parent_fidstr = parent_fidstr; });
        change_map_fidstr.modify(iter, [object_name](change_descriptor &cd){ cd.object_name = object_name; });
        change_map_fidstr.modify(iter, [lustre_path](change_descriptor &cd){ cd.lustre_path = lustre_path; });
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.oper_complete = false; });
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.last_event = ChangeDescriptor::EventTypeEnum::CREATE; });
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
    } else {
        change_descriptor entry{};
        entry.fidstr = fidstr;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        entry.lustre_path = lustre_path;
        entry.oper_complete = false;
        entry.last_event = ChangeDescriptor::EventTypeEnum::CREATE;
        entry.timestamp = time(NULL);
        entry.object_type = ChangeDescriptor::ObjectTypeEnum::FILE;
        change_map->push_back(entry);
    }

    return lustre_irods::SUCCESS; 

}

int lustre_mtime(const char *lustre_root_path, const char *fidstr_cstr, 
        const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, void *change_map_void_ptr) {



    int rc;
    if ((rc = validate_operands(lustre_root_path, fidstr_cstr, parent_fidstr, object_name, lustre_path_cstr,change_map_void_ptr)) < 0) {
        return rc;
    }  

    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr);

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map_fidstr = change_map->get<change_descriptor_fidstr_idx>();

    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map_fidstr.find(fidstr);
    if(iter != change_map_fidstr.end()) {   
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.oper_complete = false; });
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
    } else {
        change_descriptor entry{};
        entry.fidstr = fidstr;
        //entry.parent_fidstr = parent_fidstr;
        //entry.lustre_path = lustre_path;
        //entry.object_name = object_name;
        entry.last_event = ChangeDescriptor::EventTypeEnum::OTHER;
        entry.oper_complete = false;
        entry.timestamp = time(NULL);
        change_map->push_back(entry);
    }

    return lustre_irods::SUCCESS; 

}

int lustre_trunc(const char *lustre_root_path, const char *fidstr_cstr, 
        const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, void *change_map_void_ptr) { 

    int rc;
    if ((rc = validate_operands(lustre_root_path, fidstr_cstr, parent_fidstr, object_name, lustre_path_cstr,change_map_void_ptr)) < 0) {
        return rc;
    }

    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr);

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map_fidstr = change_map->get<change_descriptor_fidstr_idx>();

    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    struct stat st;
    int result = stat(lustre_path.c_str(), &st);

    LOG(LOG_DBG, "handle_trunc:  stat_result = %i, file_size = %ld\n", result, st.st_size);

    auto iter = change_map_fidstr.find(fidstr);
    if(iter != change_map_fidstr.end()) {
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.oper_complete = false; });
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
        if (result == 0) {
            change_map_fidstr.modify(iter, [st](change_descriptor &cd){ cd.file_size = st.st_size; });
        }
    } else {
        change_descriptor entry{};
        entry.fidstr = fidstr;
        //entry.parent_fidstr = parent_fidstr;
        //entry.lustre_path = lustre_path;
        //entry.object_name = object_name;
        entry.oper_complete = false;
        entry.timestamp = time(NULL);
        if (result == 0) {
            entry.file_size = st.st_size;
        }
        change_map->push_back(entry);
    }

    return lustre_irods::SUCCESS; 


}

int remove_fidstr_from_table(const char *fidstr_cstr, void *change_map_void_ptr) {

    if (fidstr_cstr == nullptr) {
        LOG(LOG_ERR, "Null fidstr_cstr sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    if (change_map_void_ptr == nullptr) {
        LOG(LOG_ERR, "Null change_map_void_ptr sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr); 
    
    std::lock_guard<std::mutex> lock(change_table_mutex);

    std::string fidstr(fidstr_cstr);

    // get change map with index of fidstr 
    auto &change_map_fidstr = change_map->get<change_descriptor_fidstr_idx>();

    change_map_fidstr.erase(fidstr);

    return lustre_irods::SUCCESS;
}

// precondition:  result has buffer_size reserved
int concatenate_paths_with_boost(const char *p1, const char *p2, char *result, size_t buffer_size) {
 
    if (p1 == nullptr) {
        LOG(LOG_ERR, "Null p1 in %s - %d\n", __FUNCTION__, __LINE__);    
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    if (p2 == nullptr) {
        LOG(LOG_ERR, "Null p2 in %s - %d\n", __FUNCTION__, __LINE__); 
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    if (result == nullptr) {
        LOG(LOG_ERR, "Null result in %s - %d\n", __FUNCTION__, __LINE__); 
        return lustre_irods::INVALID_OPERAND_ERROR;
    }


    boost::filesystem::path path_obj_1{p1};
    boost::filesystem::path path_obj_2{p2};
    boost::filesystem::path path_result(path_obj_1/path_obj_2);

    snprintf(result, buffer_size, "%s", path_result.string().c_str());

    return lustre_irods::SUCCESS;
}



} // end extern "C"

// This is just a debugging function
void lustre_write_change_table_to_str(char *buffer, const size_t buffer_size, const change_map_t *change_map) {

    if (buffer == nullptr || change_map == nullptr) {
        return;
    }

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with sequenced index  
    auto &change_map_seq = change_map->get<change_descriptor_seq_idx>();

    char time_str[18];
    char temp_buffer[buffer_size];

    buffer[0] = '\0';

    snprintf(temp_buffer, buffer_size, "%-30s %-30s %-12s %-20s %-30s %-17s %-11s %-15s %-10s\n", "FIDSTR", 
            "PARENT_FIDSTR", "OBJECT_TYPE", "OBJECT_NAME", "LUSTRE_PATH", "TIME", "EVENT_TYPE", "OPER_COMPLETE?", "FILE_SIZE");
    strncat(buffer, temp_buffer, buffer_size-strlen(buffer)-1);
    snprintf(temp_buffer, buffer_size, "%-30s %-30s %-12s %-20s %-30s %-17s %-11s %-15s %-10s\n", "------", 
            "-------------", "-----------", "-----------", "-----------", "----", "----------", "--------------", "---------");
    strncat(buffer, temp_buffer, buffer_size-strlen(buffer)-1);
    for (auto iter = change_map_seq.begin(); iter != change_map_seq.end(); ++iter) {
         std::string fidstr = iter->fidstr;

         struct tm *timeinfo;
         timeinfo = localtime(&iter->timestamp);
         strftime(time_str, sizeof(time_str), "%Y%m%d %I:%M:%S", timeinfo);

         snprintf(temp_buffer, buffer_size, "%-30s %-30s %-12s %-20s %-30s %-17s %-11s %-15s %lu\n", fidstr.c_str(), iter->parent_fidstr.c_str(), 
                 object_type_to_str(iter->object_type).c_str(), 
                 iter->object_name.c_str(),
                 iter->lustre_path.c_str(), time_str, 
                 event_type_to_str(iter->last_event).c_str(), 
                 iter->oper_complete == 1 ? "true" : "false", iter->file_size);

         strncat(buffer, temp_buffer, buffer_size-strlen(buffer)-1);
    }

}

// This is just a debugging function
void lustre_print_change_table(change_map_t *change_map) {
    
    if (change_map == nullptr) {
        return;
    } 

    char buffer[5012];
    lustre_write_change_table_to_str(buffer, 5012, change_map);
    LOG(LOG_DBG, "%s", buffer);
}


// Processes change table by writing records ready to be sent to iRODS into capnproto buffer (buf).
// The size of the buffer is written to buflen.
// Note:  The buf is malloced and must be freed by caller.
int write_change_table_to_capnproto_buf(const lustre_irods_connector_cfg_t *config_struct_ptr, void*& buf, int& buflen, 
        change_map_t *change_map) {

    if (config_struct_ptr == nullptr) {
        LOG(LOG_ERR, "Null config_struct_ptr sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    if (change_map == nullptr) {
        LOG(LOG_ERR, "Null change_map sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with sequenced index  
    auto &change_map_seq = change_map->get<change_descriptor_seq_idx>();

    //initialize capnproto message
    capnp::MallocMessageBuilder message;
    ChangeMap::Builder changeMap = message.initRoot<ChangeMap>();

    changeMap.setLustreRootPath(config_struct_ptr->lustre_root_path);
    changeMap.setResourceId(config_struct_ptr->irods_resource_id);
    changeMap.setRegisterPath(config_struct_ptr->irods_register_path);

    size_t write_count = change_map_seq.size() >= config_struct_ptr->maximum_records_per_update_to_irods 
        ? config_struct_ptr->maximum_records_per_update_to_irods : change_map_seq.size() ;

    capnp::List<ChangeDescriptor>::Builder entries = changeMap.initEntries(write_count);

    unsigned long cnt = 0;
    for (auto iter = change_map_seq.begin(); iter != change_map_seq.end() && cnt < write_count;) { 

        LOG(LOG_DBG, "fidstr=%s oper_complete=%i\n", iter->fidstr.c_str(), iter->oper_complete);

        LOG(LOG_DBG, "change_map size = %lu\n", change_map_seq.size()); 

        if (iter->oper_complete) {
            entries[cnt].setFidstr(iter->fidstr);
            entries[cnt].setParentFidstr(iter->parent_fidstr);
            entries[cnt].setObjectType(iter->object_type);
            entries[cnt].setObjectName(iter->object_name);
            entries[cnt].setLustrePath(iter->lustre_path);
            entries[cnt].setEventType(iter->last_event);
            entries[cnt].setFileSize(iter->file_size);

            // before deleting write the entry to removed_entries 
            //removed_entries->push_back(*iter);

            // delete entry from table 
            iter = change_map_seq.erase(iter);

            ++cnt;

            LOG(LOG_DBG, "after erase change_map size = %lu\n", change_map_seq.size());

        } else {
            ++iter;
        }
        
    }


    LOG(LOG_DBG, "write_count=%lu cnt=%lu\n", write_count, cnt);

    kj::Array<capnp::word> array = capnp::messageToFlatArray(message);
    size_t message_size = array.size() * sizeof(capnp::word);

    LOG(LOG_DBG, "message_size=%lu\n", message_size);

    buf = (unsigned char*)malloc(message_size);
    buflen = message_size;
    memcpy(buf, std::begin(array), message_size);

    return lustre_irods::SUCCESS;
}

// If we get a failure, the accumulator needs to add the entry back to the list.
int add_capnproto_buffer_back_to_change_table(unsigned char* buf, int buflen, change_map_t *change_map) {

    const kj::ArrayPtr<const capnp::word> array_ptr{ reinterpret_cast<const capnp::word*>(&(*(buf))),
        reinterpret_cast<const capnp::word*>(&(*(buf + buflen)))};
    capnp::FlatArrayMessageReader message(array_ptr);

    ChangeMap::Reader changeMap = message.getRoot<ChangeMap>();

    for (ChangeDescriptor::Reader entry : changeMap.getEntries()) {

        change_descriptor record {};
        record.last_event = entry.getEventType();
        record.fidstr = entry.getFidstr().cStr();
        record.lustre_path = entry.getLustrePath().cStr();
        record.object_name = entry.getObjectName().cStr();
        record.object_type = entry.getObjectType();
        record.parent_fidstr = entry.getParentFidstr().cStr();
        record.file_size = entry.getFileSize();
        record.oper_complete = true;
        record.timestamp = time(NULL);

        LOG(LOG_DBG, "writing entry back to change_map.\n");

        // TODO - do we need to put this on the front of the list?
        change_map->push_back(record);
    }

    return lustre_irods::SUCCESS;
}   
std::string event_type_to_str(ChangeDescriptor::EventTypeEnum type) {
    switch (type) {
        case ChangeDescriptor::EventTypeEnum::OTHER:
            return "OTHER";
            break;
        case ChangeDescriptor::EventTypeEnum::CREATE:
            return "CREATE";
            break;
        case ChangeDescriptor::EventTypeEnum::UNLINK:
            return "UNLINK";
            break;
        case ChangeDescriptor::EventTypeEnum::RMDIR:
            return "RMDIR";
            break;
        case ChangeDescriptor::EventTypeEnum::MKDIR:
            return "MKDIR";
            break;
        case ChangeDescriptor::EventTypeEnum::RENAME:
            return "RENAME";
            break;
    }
    return "";
}

ChangeDescriptor::EventTypeEnum str_to_event_type(const std::string& str) {
    if (str == "CREATE") {
        return ChangeDescriptor::EventTypeEnum::CREATE;
    } else if (str == "UNLINK") {
        return ChangeDescriptor::EventTypeEnum::UNLINK;
    } else if (str == "RMDIR") {
        return ChangeDescriptor::EventTypeEnum::RMDIR;
    } else if (str == "MKDIR") {
        return ChangeDescriptor::EventTypeEnum::MKDIR;
    } else if (str == "RENAME") {
        return ChangeDescriptor::EventTypeEnum::RENAME;
    }
    return ChangeDescriptor::EventTypeEnum::OTHER;
}

std::string object_type_to_str(ChangeDescriptor::ObjectTypeEnum type) {
    switch (type) {
        case ChangeDescriptor::ObjectTypeEnum::FILE: 
            return "FILE";
            break;
        case ChangeDescriptor::ObjectTypeEnum::DIR:
            return "DIR";
            break;
    }
    return "";
}

ChangeDescriptor::ObjectTypeEnum str_to_object_type(const std::string& str) {
    if (str == "DIR")  {
        return ChangeDescriptor::ObjectTypeEnum::DIR;
   }
   return ChangeDescriptor::ObjectTypeEnum::FILE;
} 

bool entries_ready_to_process(change_map_t *change_map) {

    std::lock_guard<std::mutex> lock(change_table_mutex);

    if (change_map == nullptr) {
        LOG(LOG_DBG, "change map null pointer received\n");
        return false;
    }

    // get change map indexed on oper_complete 
    auto &change_map_oper_complete = change_map->get<change_descriptor_oper_complete_idx>();
    bool ready = change_map_oper_complete.count(true) > 0;
    LOG(LOG_INFO, "entries_ready_to_process = %i\n", ready);
    return ready; 
}

int serialize_change_map_to_sqlite(change_map_t *change_map) {

    if (change_map == nullptr) {
        LOG(LOG_ERR, "Null change_map %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    std::lock_guard<std::mutex> lock(change_table_mutex);

    sqlite3 *db;
    int rc;

    rc = sqlite3_open("change_map.db", &db);

    if (rc) {
        LOG(LOG_ERR, "Can't open change_map.db for serialization.\n");
        return lustre_irods::SQLITE_DB_ERROR;
    }

    // get change map with sequenced index  
    auto &change_map_seq = change_map->get<change_descriptor_seq_idx>();

    for (auto iter = change_map_seq.begin(); iter != change_map_seq.end(); ++iter) {  

        sqlite3_stmt *stmt;     
        sqlite3_prepare_v2(db, "insert into change_map (fidstr, parent_fidstr, object_name, lustre_path, last_event, "
                               "timestamp, oper_complete, object_type, file_size) values (?1, ?2, ?3, ?4, "
                               "?5, ?6, ?7, ?8, ?9);", -1, &stmt, NULL);       
        sqlite3_bind_text(stmt, 1, iter->fidstr.c_str(), -1, SQLITE_STATIC); 
        sqlite3_bind_text(stmt, 2, iter->parent_fidstr.c_str(), -1, SQLITE_STATIC); 
        sqlite3_bind_text(stmt, 3, iter->object_name.c_str(), -1, SQLITE_STATIC); 
        sqlite3_bind_text(stmt, 4, iter->lustre_path.c_str(), -1, SQLITE_STATIC); 
        sqlite3_bind_text(stmt, 5, event_type_to_str(iter->last_event).c_str(), -1, SQLITE_STATIC); 
        sqlite3_bind_int(stmt, 6, iter->timestamp); 
        sqlite3_bind_int(stmt, 7, iter->oper_complete ? 1 : 0);
        sqlite3_bind_text(stmt, 8, object_type_to_str(iter->object_type).c_str(), -1, SQLITE_STATIC); 
        sqlite3_bind_int(stmt, 9, iter->file_size); 

        rc = sqlite3_step(stmt); 
        if (rc != SQLITE_DONE) {
            LOG(LOG_ERR, "ERROR inserting data: %s\n", sqlite3_errmsg(db));
        }

        sqlite3_finalize(stmt);
    }

    sqlite3_close(db);

    return lustre_irods::SUCCESS;
}

static int query_callback(void* change_map_void_pointer, int argc, char** argv, char** columnNames) {

    if (change_map_void_pointer  == nullptr) {
        LOG(LOG_ERR, "Null change_map_void_pointer %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_pointer);

    if (argc != 9) {
        LOG(LOG_ERR, "Invalid number of columns returned from change_map query in database.\n");
        return  lustre_irods::SQLITE_DB_ERROR;
    }

    change_descriptor entry{};
    entry.fidstr = argv[0];
    entry.parent_fidstr = argv[1];
    entry.object_name = argv[2];
    entry.object_type = str_to_object_type(argv[3]);
    entry.lustre_path = argv[4]; 

    int oper_complete;
    int timestamp;
    int file_size;

    try {
        oper_complete = boost::lexical_cast<int>(argv[5]);
        timestamp = boost::lexical_cast<time_t>(argv[6]);
        file_size = boost::lexical_cast<off_t>(argv[8]);
    } catch( boost::bad_lexical_cast const& ) {
        LOG(LOG_ERR, "Could not convert the string to int returned from change_map query in database.\n");
        return  lustre_irods::SQLITE_DB_ERROR;
    }

    entry.oper_complete = (oper_complete == 1);
    entry.timestamp = timestamp;
    entry.last_event = str_to_event_type(argv[7]);
    entry.file_size = file_size;

    std::lock_guard<std::mutex> lock(change_table_mutex);
    change_map->push_back(entry);

    return lustre_irods::SUCCESS;
}

int deserialize_change_map_from_sqlite(change_map_t *change_map) {

    if (change_map == nullptr) {
        LOG(LOG_ERR, "Null change_map %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    sqlite3 *db;
    char *zErrMsg = 0;
    int rc;

    rc = sqlite3_open("change_map.db", &db);

    if (rc) {
        LOG(LOG_ERR, "Can't open change_map.db for de-serialization.\n");
        return lustre_irods::SQLITE_DB_ERROR;
    }

    rc = sqlite3_exec(db, "select fidstr, parent_fidstr, object_name, object_type, lustre_path, oper_complete, "
                          "timestamp, last_event, file_size from change_map", query_callback, change_map, &zErrMsg);

    if (rc) {
        LOG(LOG_ERR, "Error querying change_map from db during de-serialization: %s\n", zErrMsg);
        sqlite3_close(db);
        return lustre_irods::SQLITE_DB_ERROR;
    }

    // delete contents of table using sqlite truncate optimizer
    rc = sqlite3_exec(db, "delete from change_map", NULL, NULL, &zErrMsg);
    
    if (rc) {
        LOG(LOG_ERR, "Error clearing out change_map from db during de-serialization: %s\n", zErrMsg);
        sqlite3_close(db);
        return lustre_irods::SQLITE_DB_ERROR;
    }

    sqlite3_close(db);

    return lustre_irods::SUCCESS;
}

int initiate_change_map_serialization_database() {

    sqlite3 *db;
    char *zErrMsg = 0;
    int rc;

    const char *create_table_str = "create table if not exists change_map ("
       "fidstr char(256) primary key, "
       "parent_fidstr char(256), "
       "object_name char(256), "
       "lustre_path char(256), "
       "last_event char(256), "
       "timestamp integer, "
       "oper_complete integer, "
       "object_type char(256), "
       "file_size integer)";
 
    rc = sqlite3_open("change_map.db", &db);

    if (rc) {
        LOG(LOG_ERR, "Can't create or open change_map.db.\n");
        return lustre_irods::SQLITE_DB_ERROR;
    }

    rc = sqlite3_exec(db, create_table_str,  NULL, NULL, &zErrMsg);
    
    if (rc) {
        LOG(LOG_ERR, "Error creating change_map table: %s\n", zErrMsg);
        sqlite3_close(db);
        return lustre_irods::SQLITE_DB_ERROR;
    }

    sqlite3_close(db);

    return lustre_irods::SUCCESS;
}

void add_entries_back_to_change_table(change_map_t *change_map, std::shared_ptr<change_map_t>& removed_entries) {

    std::lock_guard<std::mutex> lock(change_table_mutex);

    auto &change_map_seq = removed_entries->get<0>(); 
    for (auto iter = change_map_seq.begin(); iter != change_map_seq.end(); ++iter) {
        change_map->push_back(*iter);
    }
}

