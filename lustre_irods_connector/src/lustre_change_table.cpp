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
//#include "../../irods_lustre_api/src/inout_structs.h"
#include "inout_structs.h"
#include "logging.hpp"
#include "config.hpp"

// capnproto
#include "change_table.capnp.h"
#include <capnp/message.h>
#include <capnp/serialize-packed.h>

std::string event_type_to_str(ChangeDescriptor::EventTypeEnum type);
std::string object_type_to_str(ChangeDescriptor::ObjectTypeEnum type);


//using namespace boost::interprocess;

//static boost::shared_mutex change_table_mutex;
std::mutex change_table_mutex;

extern "C" {

void lustre_close(const lustre_irods_connector_cfg_t *config_struct_ptr, const char *fidstr_cstr, 
        const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, void *change_map_void_ptr) {

    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr);

    std::lock_guard<std::mutex> lock(change_table_mutex);
 
    // get change map with hashed index of fidstr
    auto &change_map_fidstr = change_map->get<1>();
    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    struct stat st;
    int result = stat(lustre_path.c_str(), &st);

    LOG(LOG_DBG, "stat(%s, &st)\n", lustre_path.c_str());
    LOG(LOG_DBG, "handle_close:  stat_result = %i, file_size = %ld\n", result, st.st_size);

    auto iter = change_map_fidstr.find(fidstr);
    if(iter != change_map_fidstr.end()) {
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.oper_complete = true; });
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
        if (result == 0)
            change_map_fidstr.modify(iter, [st](change_descriptor &cd){ cd.file_size = st.st_size; });
    } else {
        // this is probably an append so no file update is done
        change_descriptor entry;
        memset(&entry, 0, sizeof(entry));
        entry.fidstr = fidstr;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        entry.object_type = (result == 0 && S_ISDIR(st.st_mode)) ? ChangeDescriptor::ObjectTypeEnum::DIR : ChangeDescriptor::ObjectTypeEnum::FILE;
        entry.lustre_path = lustre_path; 
        entry.oper_complete = true;
        entry.timestamp = time(NULL);
        entry.last_event = ChangeDescriptor::EventTypeEnum::OTHER;
        if (result == 0)
            entry.file_size = st.st_size;

        // todo is this getting deleted
        change_map->push_back(entry);
    }

}

void lustre_mkdir(const lustre_irods_connector_cfg_t *config_struct_ptr, const char *fidstr_cstr, 
        const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, void *change_map_void_ptr) {

    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr);

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map_fidstr = change_map->get<1>();
    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map_fidstr.find(fidstr);
    if(iter != change_map_fidstr.end()) {
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.oper_complete = true; });
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.last_event = ChangeDescriptor::EventTypeEnum::MKDIR; });
    } else {
        change_descriptor entry;
        memset(&entry, 0, sizeof(entry));
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

}

void lustre_rmdir(const lustre_irods_connector_cfg_t *config_struct_ptr, const char *fidstr_cstr, 
        const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, void *change_map_void_ptr) {

    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr);

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map_fidstr = change_map->get<1>();

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
        change_descriptor entry;
        memset(&entry, 0, sizeof(entry));
        entry.fidstr = fidstr;
        entry.oper_complete = true;
        entry.last_event = ChangeDescriptor::EventTypeEnum::RMDIR;
        entry.timestamp = time(NULL);
        entry.object_type = ChangeDescriptor::ObjectTypeEnum::DIR;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        change_map->push_back(entry);
    }

}

void lustre_unlink(const lustre_irods_connector_cfg_t *config_struct_ptr, const char *fidstr_cstr, 
        const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, void *change_map_void_ptr) {

    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr);

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map_fidstr = change_map->get<1>();

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
        change_descriptor entry;
        memset(&entry, 0, sizeof(entry));
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


}

void lustre_rename(const lustre_irods_connector_cfg_t *config_struct_ptr, const char *fidstr_cstr, 
        const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, 
        const char *old_lustre_path_cstr, void *change_map_void_ptr) {

    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr);

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map_fidstr = change_map->get<1>();

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
        change_descriptor entry;
        memset(&entry, 0, sizeof(entry));
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

}

void lustre_create(const lustre_irods_connector_cfg_t *config_struct_ptr, const char *fidstr_cstr, 
        const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, void *change_map_void_ptr) {

    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr);

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map_fidstr = change_map->get<1>();

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
        change_descriptor entry;
        memset(&entry, 0, sizeof(entry));
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

}

void lustre_mtime(const lustre_irods_connector_cfg_t *config_struct_ptr, const char *fidstr_cstr, 
        const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, void *change_map_void_ptr) {

    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr);

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map_fidstr = change_map->get<1>();

    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map_fidstr.find(fidstr);
    if(iter != change_map_fidstr.end()) {   
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.oper_complete = false; });
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
    } else {
        change_descriptor entry;
        memset(&entry, 0, sizeof(entry));
        entry.fidstr = fidstr;
        //entry.parent_fidstr = parent_fidstr;
        //entry.lustre_path = lustre_path;
        //entry.object_name = object_name;
        entry.last_event = ChangeDescriptor::EventTypeEnum::OTHER;
        entry.oper_complete = false;
        entry.timestamp = time(NULL);
        change_map->push_back(entry);
    }


}

void lustre_trunc(const lustre_irods_connector_cfg_t *config_struct_ptr, const char *fidstr_cstr, 
        const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, void *change_map_void_ptr) { 

    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr);

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map_fidstr = change_map->get<1>();

    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    struct stat st;
    int result = stat(lustre_path.c_str(), &st);

    LOG(LOG_DBG, "handle_trunc:  stat_result = %i, file_size = %ld\n", result, st.st_size);

    auto iter = change_map_fidstr.find(fidstr);
    if(iter != change_map_fidstr.end()) {
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.oper_complete = false; });
        change_map_fidstr.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
        if (result == 0)
            change_map_fidstr.modify(iter, [st](change_descriptor &cd){ cd.file_size = st.st_size; });
    } else {
        change_descriptor entry;
        memset(&entry, 0, sizeof(entry));
        entry.fidstr = fidstr;
        //entry.parent_fidstr = parent_fidstr;
        //entry.lustre_path = lustre_path;
        //entry.object_name = object_name;
        entry.oper_complete = false;
        entry.timestamp = time(NULL);
        if (result == 0)
            entry.file_size = st.st_size;
        change_map->push_back(entry);
    }
}

void remove_fidstr_from_table(const char *fidstr_cstr, void *change_map_void_ptr) {

    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr); 
    
    std::string fidstr(fidstr_cstr);

    // get change map with index of fidstr 
    auto &change_map_fidstr = change_map->get<1>();

    change_map_fidstr.erase(fidstr);
}

// precondition:  p3 has buffer_size reserved
int concatenate_paths_with_boost(const char *p1, const char *p2, char *result, size_t buffer_size) {
 
    if (p1 == nullptr || p2 == nullptr) {
        return -1;
    }

    boost::filesystem::path path_obj_1{p1};
    boost::filesystem::path path_obj_2{p2};
    boost::filesystem::path path_result(path_obj_1/path_obj_2);

    snprintf(result, buffer_size, "%s", path_result.string().c_str());

    return 0;
}



} // end extern "C"

void lustre_write_change_table_to_str(char *buffer, const size_t buffer_size, const change_map_t *change_map) {

    //boost::shared_lock<boost::shared_mutex> lock(change_table_mutex);
    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with sequenced index  
    auto &change_map_seq = change_map->get<0>();

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

void lustre_print_change_table(change_map_t *change_map) {
    char buffer[5012];
    lustre_write_change_table_to_str(buffer, 5012, change_map);
    LOG(LOG_DBG, "%s", buffer);
}


// processes change table by writing records ready to be sent to iRODS into 
// irodsLustreApiInp_t structure formatted by capnproto.
// Note:  The irodsLustreApiInp_t::buf is malloced and must be freed by caller.
void write_change_table_to_capnproto_buf(const lustre_irods_connector_cfg_t *config_struct_ptr, irodsLustreApiInp_t *inp, change_map_t *change_map) {

    //boost::shared_lock<boost::shared_mutex> lock(change_table_mutex);
    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with sequenced index  
    auto &change_map_seq = change_map->get<0>();

    //initialize capnproto message
    capnp::MallocMessageBuilder message;
    ChangeMap::Builder changeMap = message.initRoot<ChangeMap>();

    changeMap.setLustreRootPath(config_struct_ptr->lustre_root_path);
    changeMap.setResourceId(config_struct_ptr->irods_resource_id);
    changeMap.setRegisterPath(config_struct_ptr->irods_register_path);

    size_t write_count = change_map_seq.size();
    capnp::List<ChangeDescriptor>::Builder entries = changeMap.initEntries(write_count);

    unsigned long cnt = 0;
    for (auto iter = change_map_seq.begin(); iter != change_map_seq.end();) {

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

            // delete entry from table 
            iter = change_map_seq.erase(iter);

            ++cnt;

            LOG(LOG_DBG, "after erase change_map size = %lu\n", change_map_seq.size());

        } else {
            ++iter;
        }
        
    }

    kj::Array<capnp::word> array = capnp::messageToFlatArray(message);
    size_t message_size = array.size() * sizeof(capnp::word);

    inp->buf = (unsigned char*)malloc(message_size);
    inp->buflen = message_size;
    memcpy(inp->buf, std::begin(array), message_size);
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

bool entries_ready_to_process(change_map_t *change_map) {

    if (change_map == nullptr) {
        LOG(LOG_DBG, "change map null pointer received\n");
    }

    // get change map with size index
    auto &change_map_size = change_map->get<2>();
    bool ready = change_map_size.count(true) > 0;
    LOG(LOG_INFO, "entries_ready_to_process = %i\n", ready);
    return ready; 
}

