#include <string>
#include <ctime>
#include <sstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

// boost headers
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread.hpp>
//#include <boost/interprocess/sync/named_mutex.hpp>
//#include <boost/interprocess/sync/named_upgradable_mutex.hpp>

#include "lustre_change_table.hpp"
#include "../../irods_lustre_api/src/inout_structs.h"

// capnproto
#include "change_table.capnp.h"
#include <capnp/message.h>
#include <capnp/serialize-packed.h>

std::string event_type_to_str(ChangeDescriptor::EventTypeEnum type);
std::string object_type_to_str(ChangeDescriptor::ObjectTypeEnum type);


//using namespace boost::interprocess;

extern char lustre_root_path[];
extern char register_path[];
extern int64_t resource_id;

//static std::string shared_memory_mutex_name = "change_table_mutex";
static boost::shared_mutex change_table_mutex;

extern "C" {

void lustre_close(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) {

    boost::unique_lock<boost::shared_mutex> lock(change_table_mutex);
 
    //named_upgradable_mutex named_mtx(open_or_create, shared_memory_mutex_name.c_str());
    //named_mtx.lock();

    std::map<std::string, change_descriptor> *change_map = get_change_map_instance();
    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    struct stat st;
    int result = stat(lustre_path.c_str(), &st);

    printf("stat(%s, &st)\n", lustre_path.c_str());
    printf("handle_close:  stat_result = %i, file_size = %ld\n", result, st.st_size);

    auto iter = change_map->find(fidstr);
    if(iter != change_map->end()) {
        change_descriptor& entry = iter->second;
        entry.oper_complete = true;
        entry.timestamp = time(NULL);
        if (result == 0)
            entry.file_size = st.st_size;
    } else {
        // this is probably an append so no file update is done
        change_descriptor entry;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        entry.object_type = (result == 0 && S_ISDIR(st.st_mode)) ? ChangeDescriptor::ObjectTypeEnum::DIR : ChangeDescriptor::ObjectTypeEnum::FILE;
        entry.lustre_path = lustre_path; 
        entry.oper_complete = true;
        entry.timestamp = time(NULL);
        entry.last_event = ChangeDescriptor::EventTypeEnum::OTHER;
        if (result == 0)
            entry.file_size = st.st_size;
        (*change_map)[fidstr] = entry;
    }

}

void lustre_mkdir(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) {

    boost::unique_lock<boost::shared_mutex> lock(change_table_mutex);

    std::map<std::string, change_descriptor> *change_map = get_change_map_instance();
    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map->find(fidstr);
    if(iter != change_map->end()) {
        change_descriptor& entry = iter->second;
        entry.oper_complete = true;
        entry.last_event = ChangeDescriptor::EventTypeEnum::MKDIR;
        entry.timestamp = time(NULL);
    } else {
        change_descriptor entry;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        entry.lustre_path = lustre_path;
        entry.oper_complete = true;
        entry.last_event = ChangeDescriptor::EventTypeEnum::MKDIR;
        entry.timestamp = time(NULL);
        entry.object_type = ChangeDescriptor::ObjectTypeEnum::DIR;
        (*change_map)[fidstr] = entry;
    }

}

void lustre_rmdir(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) {

    boost::unique_lock<boost::shared_mutex> lock(change_table_mutex);

    std::map<std::string, change_descriptor> *change_map = get_change_map_instance();
    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map->find(fidstr);
    if(iter != change_map->end()) {
        change_descriptor& entry = iter->second;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        entry.lustre_path = lustre_path;
        entry.oper_complete = true;
        entry.last_event = ChangeDescriptor::EventTypeEnum::RMDIR;
        entry.timestamp = time(NULL);
    } else {
        change_descriptor entry;
        entry.oper_complete = true;
        entry.last_event = ChangeDescriptor::EventTypeEnum::RMDIR;
        entry.timestamp = time(NULL);
        entry.object_type = ChangeDescriptor::ObjectTypeEnum::DIR;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        (*change_map)[fidstr] = entry;
    }

}

void lustre_unlink(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) {

    boost::unique_lock<boost::shared_mutex> lock(change_table_mutex);

    std::map<std::string, change_descriptor> *change_map = get_change_map_instance();
    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map->find(fidstr);
    if(iter != change_map->end()) {   
        change_descriptor& entry = iter->second;
        entry.oper_complete = true;
        entry.last_event = ChangeDescriptor::EventTypeEnum::UNLINK;
        entry.timestamp = time(NULL);
    } else {
        change_descriptor entry;
        //entry.parent_fidstr = parent_fidstr;
        //entry.lustre_path = lustre_path;
        entry.oper_complete = true;
        entry.last_event = ChangeDescriptor::EventTypeEnum::UNLINK;
        entry.timestamp = time(NULL);
        entry.object_type = ChangeDescriptor::ObjectTypeEnum::FILE;
        entry.object_name = object_name;
        (*change_map)[fidstr] = entry;
    }


}

void lustre_rename(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, const char *old_lustre_path_cstr) {

    boost::unique_lock<boost::shared_mutex> lock(change_table_mutex);

    std::map<std::string, change_descriptor> *change_map = get_change_map_instance();
    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);
    std::string old_lustre_path(old_lustre_path_cstr);

    auto iter = change_map->find(fidstr);
    std::string original_path;

    struct stat statbuf;
    bool is_dir = stat(lustre_path.c_str(), &statbuf) == 0 && S_ISDIR(statbuf.st_mode);

    // if there is a previous entry, just update the lustre_path to the new path
    // otherwise, add a new entry
    if(iter != change_map->end()) {
        iter->second.parent_fidstr = parent_fidstr;
        iter->second.lustre_path = lustre_path; 
        iter->second.object_name = object_name;
    } else {

        change_descriptor entry;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        entry.lustre_path = lustre_path;
        entry.oper_complete = true;
        entry.last_event = ChangeDescriptor::EventTypeEnum::RENAME;
        entry.timestamp = time(NULL);
        if (is_dir) {
            entry.object_type = ChangeDescriptor::ObjectTypeEnum::DIR;
        } else {
            entry.object_type = ChangeDescriptor::ObjectTypeEnum::FILE;
        }
        (*change_map)[fidstr] = entry;
    }

    printf("rename:  old_lustre_path = %s\n", old_lustre_path.c_str());

    if (is_dir) {

        // search through and update all references in table
        for (auto iter = change_map->begin(); iter != change_map->end(); ++iter) {
            std::string p = iter->second.lustre_path;
            if (p.length() > 0 && p.length() != old_lustre_path.length() && boost::starts_with(p, old_lustre_path)) {
               iter->second.lustre_path.replace(0, old_lustre_path.length(), lustre_path);
            }
        }
    }

}

void lustre_create(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) {

    boost::unique_lock<boost::shared_mutex> lock(change_table_mutex);

    std::map<std::string, change_descriptor> *change_map = get_change_map_instance();
    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map->find(fidstr);
    if(iter != change_map->end()) {
        change_descriptor& entry = iter->second;
        entry.oper_complete = false;
        entry.last_event = ChangeDescriptor::EventTypeEnum::CREAT;
        entry.timestamp = time(NULL);
    } else {
        change_descriptor entry;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        entry.lustre_path = lustre_path;
        entry.oper_complete = false;
        entry.last_event = ChangeDescriptor::EventTypeEnum::CREAT;
        entry.timestamp = time(NULL);
        entry.object_type = ChangeDescriptor::ObjectTypeEnum::FILE;
        (*change_map)[fidstr] = entry;
    }

}

void lustre_mtime(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) {

    boost::unique_lock<boost::shared_mutex> lock(change_table_mutex);

    std::map<std::string, change_descriptor> *change_map = get_change_map_instance();
    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map->find(fidstr);
    if(iter != change_map->end()) {   
        change_descriptor& entry = iter->second;
        entry.oper_complete = false;
        entry.timestamp = time(NULL);
    } else {
        change_descriptor entry;
        //entry.parent_fidstr = parent_fidstr;
        //entry.lustre_path = lustre_path;
        //entry.object_name = object_name;
        entry.last_event = ChangeDescriptor::EventTypeEnum::OTHER;
        entry.oper_complete = false;
        entry.timestamp = time(NULL);
        (*change_map)[fidstr] = entry;
    }


}

void lustre_trunc(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) { 

    boost::unique_lock<boost::shared_mutex> lock(change_table_mutex);

    std::map<std::string, change_descriptor> *change_map = get_change_map_instance();
    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    struct stat st;
    int result = stat(lustre_path.c_str(), &st);

    printf("handle_trunc:  stat_result = %i, file_size = %ld\n", result, st.st_size);

    auto iter = change_map->find(fidstr);
    if(iter != change_map->end()) {
        change_descriptor& entry = iter->second;
        entry.oper_complete = false;
        entry.timestamp = time(NULL);
        if (result == 0)
            entry.file_size = st.st_size;
    } else {
        change_descriptor entry;
        //entry.parent_fidstr = parent_fidstr;
        //entry.lustre_path = lustre_path;
        //entry.object_name = object_name;
        entry.oper_complete = false;
        entry.timestamp = time(NULL);
        if (result == 0)
            entry.file_size = st.st_size;
        (*change_map)[fidstr] = entry;
    }


}

void lustre_write_change_table_to_str(char *buffer, const size_t buffer_size) {

    boost::shared_lock<boost::shared_mutex> lock(change_table_mutex);

    std::map<std::string, change_descriptor> *change_map = get_change_map_instance();

    char time_str[18];
    char temp_buffer[buffer_size];

    buffer[0] = '\0';

    snprintf(temp_buffer, buffer_size, "%-30s %-30s %-12s %-20s %-30s %-17s %-11s %-15s %-10s\n", "FIDSTR", 
            "PARENT_FIDSTR", "OBJECT_TYPE", "OBJECT_NAME", "LUSTRE_PATH", "TIME", "EVENT_TYPE", "OPER_COMPLETE?", "FILE_SIZE");
    strncat(buffer, temp_buffer, buffer_size-strlen(buffer)-1);
    snprintf(temp_buffer, buffer_size, "%-30s %-30s %-12s %-20s %-30s %-17s %-11s %-15s %-10s\n", "------", 
            "-------------", "-----------", "-----------", "-----------", "----", "----------", "--------------", "---------");
    strncat(buffer, temp_buffer, buffer_size-strlen(buffer)-1);
    for (auto iter = change_map->begin(); iter != change_map->end(); ++iter) {
         std::string fidstr = iter->first;
         change_descriptor fields = iter->second;

         struct tm *timeinfo;
         timeinfo = localtime(&fields.timestamp);
         strftime(time_str, sizeof(time_str), "%Y%m%d %I:%M:%S", timeinfo);

         snprintf(temp_buffer, buffer_size, "%-30s %-30s %-12s %-20s %-30s %-17s %-11s %-15s %lu\n", fidstr.c_str(), fields.parent_fidstr.c_str(), 
                 object_type_to_str(fields.object_type).c_str(), 
                 fields.object_name.c_str(),
                 fields.lustre_path.c_str(), time_str, 
                 event_type_to_str(fields.last_event).c_str(), 
                 fields.oper_complete == 1 ? "true" : "false", fields.file_size);

         strncat(buffer, temp_buffer, buffer_size-strlen(buffer)-1);
    }

}

void lustre_print_change_table() {
    char buffer[5012];
    lustre_write_change_table_to_str(buffer, 5012);
    printf("%s", buffer);
}


// processes change table by writing records ready to be sent to iRODS into 
// irodsLustreApiInp_t structure formatted by capnproto.
// Note:  The irodsLustreApiInp_t::buf is malloced and must be freed by callerrodsLustreApiInp_t.
void write_change_table_to_capnproto_buf(irodsLustreApiInp_t *inp) {

    boost::shared_lock<boost::shared_mutex> lock(change_table_mutex);

    std::map<std::string, change_descriptor> *change_map = get_change_map_instance();

    //initialize capnproto message
    capnp::MallocMessageBuilder message;
    ChangeMap::Builder changeMap = message.initRoot<ChangeMap>();

    changeMap.setLustreRootPath(lustre_root_path);
    changeMap.setResourceId(resource_id);
    changeMap.setRegisterPath(register_path);

    // TODO investigate this
    size_t write_count = change_map->size();
    capnp::List<ChangeDescriptor>::Builder entries = changeMap.initEntries(write_count);

    unsigned long cnt = 0;
    for (auto iter = change_map->begin(); iter != change_map->end();) {

        change_descriptor fields = iter->second;

        if (fields.oper_complete) {
            entries[cnt].setFidstr(iter->first);
            entries[cnt].setParentFidstr(fields.parent_fidstr);
            entries[cnt].setObjectType(fields.object_type);
            entries[cnt].setObjectName(fields.object_name);
            entries[cnt].setLustrePath(fields.lustre_path);
            entries[cnt].setEventType(fields.last_event);
            entries[cnt].setFileSize(fields.file_size);

            // delete entry from table 
            iter = change_map->erase(iter);

            ++cnt;

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
    

}



std::map<std::string, change_descriptor> *change_map = NULL;
std::map<std::string, change_descriptor> *get_change_map_instance() {
    if (!change_map)
        change_map = new std::map<std::string, change_descriptor>();
    return change_map;
}

std::string event_type_to_str(ChangeDescriptor::EventTypeEnum type) {
    switch (type) {
        case ChangeDescriptor::EventTypeEnum::OTHER:
            return "OTHER";
            break;
        case ChangeDescriptor::EventTypeEnum::CREAT:
            return "CREAT";
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


void remove_fidstr_from_table(const char *fidstr_cstr) {
    std::string fidstr(fidstr_cstr);
    get_change_map_instance()->erase(fidstr);
}

size_t get_change_table_size() {
    return get_change_map_instance()->size();
}

