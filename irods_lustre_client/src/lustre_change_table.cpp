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

    // get change map with hashed index of fidstr
    auto &change_map = get_change_map_instance()->get<1>();
    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    struct stat st;
    int result = stat(lustre_path.c_str(), &st);

    printf("stat(%s, &st)\n", lustre_path.c_str());
    printf("handle_close:  stat_result = %i, file_size = %ld\n", result, st.st_size);

    auto iter = change_map.find(fidstr);
    if(iter != change_map.end()) {
        change_map.modify(iter, [](change_descriptor &cd){ cd.oper_complete = true; });
        change_map.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
        if (result == 0)
            change_map.modify(iter, [st](change_descriptor &cd){ cd.file_size = st.st_size; });
    } else {
        std::cout << "CLOSE did not find " << fidstr << std::endl;
        // this is probably an append so no file update is done
        change_descriptor entry;
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
        get_change_map_instance()->push_back(entry);
    }

}

void lustre_mkdir(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) {

    boost::unique_lock<boost::shared_mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map = get_change_map_instance()->get<1>();
    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map.find(fidstr);
    if(iter != change_map.end()) {
        change_map.modify(iter, [](change_descriptor &cd){ cd.oper_complete = true; });
        change_map.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
        change_map.modify(iter, [](change_descriptor &cd){ cd.last_event = ChangeDescriptor::EventTypeEnum::MKDIR; });
    } else {
        change_descriptor entry;
        entry.fidstr = fidstr;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        entry.lustre_path = lustre_path;
        entry.oper_complete = true;
        entry.last_event = ChangeDescriptor::EventTypeEnum::MKDIR;
        entry.timestamp = time(NULL);
        entry.object_type = ChangeDescriptor::ObjectTypeEnum::DIR;
        get_change_map_instance()->push_back(entry);
    }

}

void lustre_rmdir(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) {

    boost::unique_lock<boost::shared_mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map = get_change_map_instance()->get<1>();

    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map.find(fidstr);
    if(iter != change_map.end()) {
        change_map.modify(iter, [parent_fidstr](change_descriptor &cd){ cd.parent_fidstr = parent_fidstr; });
        change_map.modify(iter, [object_name](change_descriptor &cd){ cd.object_name = object_name; });
        change_map.modify(iter, [lustre_path](change_descriptor &cd){ cd.lustre_path = lustre_path; });
        change_map.modify(iter, [](change_descriptor &cd){ cd.oper_complete = true; });
        change_map.modify(iter, [](change_descriptor &cd){ cd.last_event = ChangeDescriptor::EventTypeEnum::RMDIR; });
        change_map.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
    } else {
        change_descriptor entry;
        entry.fidstr = fidstr;
        entry.oper_complete = true;
        entry.last_event = ChangeDescriptor::EventTypeEnum::RMDIR;
        entry.timestamp = time(NULL);
        entry.object_type = ChangeDescriptor::ObjectTypeEnum::DIR;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        get_change_map_instance()->push_back(entry);
    }

}

void lustre_unlink(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) {

    boost::unique_lock<boost::shared_mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map = get_change_map_instance()->get<1>();

    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map.find(fidstr);
    if(iter != change_map.end()) {   
        change_map.modify(iter, [](change_descriptor &cd){ cd.oper_complete = true; });
        change_map.modify(iter, [](change_descriptor &cd){ cd.last_event = ChangeDescriptor::EventTypeEnum::UNLINK; });
        change_map.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
    } else {
        change_descriptor entry;
        entry.fidstr = fidstr;
        //entry.parent_fidstr = parent_fidstr;
        //entry.lustre_path = lustre_path;
        entry.oper_complete = true;
        entry.last_event = ChangeDescriptor::EventTypeEnum::UNLINK;
        entry.timestamp = time(NULL);
        entry.object_type = ChangeDescriptor::ObjectTypeEnum::FILE;
        entry.object_name = object_name;
        get_change_map_instance()->push_back(entry);
    }


}

void lustre_rename(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, const char *old_lustre_path_cstr) {

    boost::unique_lock<boost::shared_mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map = get_change_map_instance()->get<1>();

    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);
    std::string old_lustre_path(old_lustre_path_cstr);

    auto iter = change_map.find(fidstr);
    std::string original_path;

    struct stat statbuf;
    bool is_dir = stat(lustre_path.c_str(), &statbuf) == 0 && S_ISDIR(statbuf.st_mode);

    // if there is a previous entry, just update the lustre_path to the new path
    // otherwise, add a new entry
    if(iter != change_map.end()) {
        change_map.modify(iter, [parent_fidstr](change_descriptor &cd){ cd.parent_fidstr = parent_fidstr; });
        change_map.modify(iter, [object_name](change_descriptor &cd){ cd.object_name = object_name; });
        change_map.modify(iter, [lustre_path](change_descriptor &cd){ cd.lustre_path = lustre_path; });
    } else {
        change_descriptor entry;
        entry.fidstr = fidstr;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        entry.lustre_path = lustre_path;
        entry.oper_complete = true;
        entry.last_event = ChangeDescriptor::EventTypeEnum::RENAME;
        entry.timestamp = time(NULL);
        if (is_dir) {
            change_map.modify(iter, [](change_descriptor &cd){ cd.object_type = ChangeDescriptor::ObjectTypeEnum::DIR; });
        } else {
            change_map.modify(iter, [](change_descriptor &cd){ cd.object_type = ChangeDescriptor::ObjectTypeEnum::FILE; });
        }
        get_change_map_instance()->push_back(entry);
    }

    printf("rename:  old_lustre_path = %s\n", old_lustre_path.c_str());

    if (is_dir) {

        // search through and update all references in table
        for (auto iter = change_map.begin(); iter != change_map.end(); ++iter) {
            std::string p = iter->lustre_path;
            if (p.length() > 0 && p.length() != old_lustre_path.length() && boost::starts_with(p, old_lustre_path)) {
                //TODO test
                change_map.modify(iter, [old_lustre_path, lustre_path](change_descriptor &cd){ cd.lustre_path.replace(0, old_lustre_path.length(), lustre_path); });
                //iter->lustre_path.replace(0, old_lustre_path.length(), lustre_path);
            }
        }
    }

}

void lustre_create(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) {

    boost::unique_lock<boost::shared_mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map = get_change_map_instance()->get<1>();

    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map.find(fidstr);
    if(iter != change_map.end()) {
        change_map.modify(iter, [parent_fidstr](change_descriptor &cd){ cd.parent_fidstr = parent_fidstr; });
        change_map.modify(iter, [object_name](change_descriptor &cd){ cd.object_name = object_name; });
        change_map.modify(iter, [lustre_path](change_descriptor &cd){ cd.lustre_path = lustre_path; });
        change_map.modify(iter, [](change_descriptor &cd){ cd.oper_complete = false; });
        change_map.modify(iter, [](change_descriptor &cd){ cd.last_event = ChangeDescriptor::EventTypeEnum::CREAT; });
        change_map.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
    } else {
        change_descriptor entry;
        entry.fidstr = fidstr;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        entry.lustre_path = lustre_path;
        entry.oper_complete = false;
        entry.last_event = ChangeDescriptor::EventTypeEnum::CREAT;
        entry.timestamp = time(NULL);
        entry.object_type = ChangeDescriptor::ObjectTypeEnum::FILE;
        get_change_map_instance()->push_back(entry);
    }

}

void lustre_mtime(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) {

    boost::unique_lock<boost::shared_mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map = get_change_map_instance()->get<1>();

    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map.find(fidstr);
    if(iter != change_map.end()) {   
        change_map.modify(iter, [](change_descriptor &cd){ cd.oper_complete = false; });
        change_map.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
    } else {
        change_descriptor entry;
        entry.fidstr = fidstr;
        //entry.parent_fidstr = parent_fidstr;
        //entry.lustre_path = lustre_path;
        //entry.object_name = object_name;
        entry.last_event = ChangeDescriptor::EventTypeEnum::OTHER;
        entry.oper_complete = false;
        entry.timestamp = time(NULL);
        get_change_map_instance()->push_back(entry);
    }


}

void lustre_trunc(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) { 

    boost::unique_lock<boost::shared_mutex> lock(change_table_mutex);

    // get change map with hashed index of fidstr
    auto &change_map = get_change_map_instance()->get<1>();

    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    struct stat st;
    int result = stat(lustre_path.c_str(), &st);

    printf("handle_trunc:  stat_result = %i, file_size = %ld\n", result, st.st_size);

    auto iter = change_map.find(fidstr);
    if(iter != change_map.end()) {
        change_map.modify(iter, [](change_descriptor &cd){ cd.oper_complete = false; });
        change_map.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
        if (result == 0)
            change_map.modify(iter, [st](change_descriptor &cd){ cd.file_size = st.st_size; });
    } else {
        change_descriptor entry;
        entry.fidstr = fidstr;
        //entry.parent_fidstr = parent_fidstr;
        //entry.lustre_path = lustre_path;
        //entry.object_name = object_name;
        entry.oper_complete = false;
        entry.timestamp = time(NULL);
        if (result == 0)
            entry.file_size = st.st_size;
        get_change_map_instance()->push_back(entry);
    }


}

void lustre_write_change_table_to_str(char *buffer, const size_t buffer_size) {

    boost::shared_lock<boost::shared_mutex> lock(change_table_mutex);

    // get change map with sequenced index  
    auto &change_map = get_change_map_instance()->get<0>();

    char time_str[18];
    char temp_buffer[buffer_size];

    buffer[0] = '\0';

    snprintf(temp_buffer, buffer_size, "%-30s %-30s %-12s %-20s %-30s %-17s %-11s %-15s %-10s\n", "FIDSTR", 
            "PARENT_FIDSTR", "OBJECT_TYPE", "OBJECT_NAME", "LUSTRE_PATH", "TIME", "EVENT_TYPE", "OPER_COMPLETE?", "FILE_SIZE");
    strncat(buffer, temp_buffer, buffer_size-strlen(buffer)-1);
    snprintf(temp_buffer, buffer_size, "%-30s %-30s %-12s %-20s %-30s %-17s %-11s %-15s %-10s\n", "------", 
            "-------------", "-----------", "-----------", "-----------", "----", "----------", "--------------", "---------");
    strncat(buffer, temp_buffer, buffer_size-strlen(buffer)-1);
    for (auto iter = change_map.begin(); iter != change_map.end(); ++iter) {
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

    // get change map with sequenced index  
    auto &change_map = get_change_map_instance()->get<0>();

    //initialize capnproto message
    capnp::MallocMessageBuilder message;
    ChangeMap::Builder changeMap = message.initRoot<ChangeMap>();

    changeMap.setLustreRootPath(lustre_root_path);
    changeMap.setResourceId(resource_id);
    changeMap.setRegisterPath(register_path);

    size_t write_count = change_map.size();
    capnp::List<ChangeDescriptor>::Builder entries = changeMap.initEntries(write_count);

    unsigned long cnt = 0;
    for (auto iter = change_map.begin(); iter != change_map.end();) {

        printf("fidstr=%s oper_complete=%i\n", iter->fidstr.c_str(), iter->oper_complete);

        std::cout << "change_map size = " << change_map.size() << std::endl;

        if (iter->oper_complete) {
            entries[cnt].setFidstr(iter->fidstr);
            entries[cnt].setParentFidstr(iter->parent_fidstr);
            entries[cnt].setObjectType(iter->object_type);
            entries[cnt].setObjectName(iter->object_name);
            entries[cnt].setLustrePath(iter->lustre_path);
            entries[cnt].setEventType(iter->last_event);
            entries[cnt].setFileSize(iter->file_size);

            // delete entry from table 
            iter = change_map.erase(iter);

            ++cnt;

            std::cout << "after erase change_map size = " << change_map.size() << std::endl;

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

change_map_t *change_map = NULL;
change_map_t *get_change_map_instance() {
    if (!change_map)
        change_map = new change_map_t();
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

    // get change map with index of fidstr 
    auto &change_map = get_change_map_instance()->get<1>();

    change_map.erase(fidstr);
}

bool entries_ready_to_process() {
    // get change map with size index
    auto &change_map = get_change_map_instance()->get<2>();
    bool ready = change_map.count(true) > 0;
    std::cout << "entries_ready_to_process = " << ready << std::endl;
    return ready; 
}

