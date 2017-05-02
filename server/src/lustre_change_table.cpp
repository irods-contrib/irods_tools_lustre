#include <map>
#include <string>
#include <ctime>
#include <sstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

// boost headers
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>

// json library
#include <jeayeson/jeayeson.hpp>

#include "lustre_change_table.hpp"

extern const char *lustre_root_path;
extern const char *register_path;
extern const int64_t resource_id;

extern "C" {


void lustre_close(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) {

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
        entry.object_type = (result == 0 && S_ISDIR(st.st_mode)) ? _DIR : _FILE;
        entry.lustre_path = lustre_path; 
        entry.oper_complete = true;
        entry.timestamp = time(NULL);
        entry.last_event = OTHER;
        if (result == 0)
            entry.file_size = st.st_size;
        (*change_map)[fidstr] = entry;
    }
}

void lustre_mkdir(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) {

    std::map<std::string, change_descriptor> *change_map = get_change_map_instance();
    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map->find(fidstr);
    if(iter != change_map->end()) {
        change_descriptor& entry = iter->second;
        entry.oper_complete = true;
        entry.last_event = MKDIR;
        entry.timestamp = time(NULL);
    } else {
        change_descriptor entry;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        entry.lustre_path = lustre_path;
        entry.oper_complete = true;
        entry.last_event = MKDIR;
        entry.timestamp = time(NULL);
        entry.object_type = _DIR;
        (*change_map)[fidstr] = entry;
    }
}

void lustre_rmdir(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) {

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
        entry.last_event = RMDIR;
        entry.timestamp = time(NULL);
    } else {
        change_descriptor entry;
        entry.oper_complete = true;
        entry.last_event = RMDIR;
        entry.timestamp = time(NULL);
        entry.object_type = _DIR;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        (*change_map)[fidstr] = entry;
    }
}

void lustre_unlink(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) {

    std::map<std::string, change_descriptor> *change_map = get_change_map_instance();
    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map->find(fidstr);
    if(iter != change_map->end()) {   
        change_descriptor& entry = iter->second;
        entry.oper_complete = true;
        entry.last_event = UNLINK;
        entry.timestamp = time(NULL);
    } else {
        change_descriptor entry;
        //entry.parent_fidstr = parent_fidstr;
        //entry.lustre_path = lustre_path;
        entry.oper_complete = true;
        entry.last_event = UNLINK;
        entry.timestamp = time(NULL);
        entry.object_type = _FILE;
        entry.object_name = object_name;
        (*change_map)[fidstr] = entry;
    }

}

void lustre_rename(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr, const char *old_lustre_path_cstr) {

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
        entry.last_event = RENAME;
        entry.timestamp = time(NULL);
        if (is_dir) {
            entry.object_type = _DIR;
        } else {
            entry.object_type = _FILE;
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

    std::map<std::string, change_descriptor> *change_map = get_change_map_instance();
    std::string fidstr(fidstr_cstr);
    std::string lustre_path(lustre_path_cstr);

    auto iter = change_map->find(fidstr);
    if(iter != change_map->end()) {
        change_descriptor& entry = iter->second;
        entry.oper_complete = false;
        entry.last_event = CREAT;
        entry.timestamp = time(NULL);
    } else {
        change_descriptor entry;
        entry.parent_fidstr = parent_fidstr;
        entry.object_name = object_name;
        entry.lustre_path = lustre_path;
        entry.oper_complete = false;
        entry.last_event = CREAT;
        entry.timestamp = time(NULL);
        entry.object_type = _FILE;
        (*change_map)[fidstr] = entry;
    }
}

void lustre_mtime(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) {

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
        entry.last_event = OTHER;
        entry.oper_complete = false;
        entry.timestamp = time(NULL);
        (*change_map)[fidstr] = entry;
    }

}

void lustre_trunc(const char *fidstr_cstr, const char *parent_fidstr, const char *object_name, const char *lustre_path_cstr) { 

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

    std::map<std::string, change_descriptor> *change_map = get_change_map_instance();

    char time_str[18];
    char temp_buffer[buffer_size];

    buffer[0] = '\0';

    snprintf(temp_buffer, buffer_size, "%-30s %-30s %-12s %-20s %-30s %-17s %-11s %-15s %-10s\n", "FIDSTR", 
            "PARENT_FIDSTR", "OBJECT_TYPE", "OBJECT_NAME", "LUSTRE_PATH", "TIME", "EVENT_TYPE", "OPER_COMPLETE?", "FILE_SIZE");
    strncat(buffer, temp_buffer, buffer_size);
    snprintf(temp_buffer, buffer_size, "%-30s %-30s %-12s %-20s %-30s %-17s %-11s %-15s %-10s\n", "------", 
            "-------------", "-----------", "-----------", "-----------", "----", "----------", "--------------", "---------");
    strncat(buffer, temp_buffer, buffer_size);
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

         strncat(buffer, temp_buffer, buffer_size);
    }
}

void lustre_print_change_table() {
    char buffer[5012];
    lustre_write_change_table_to_str(buffer, 5012);
    printf("%s", buffer);

}

void lustre_write_change_table_to_json_str(char *buffer, const size_t buffer_size) {

    std::map<std::string, change_descriptor> *change_map = get_change_map_instance();
    json_map change_map_json;
    jeayeson::array_t rows;

    for (auto iter = change_map->begin(); iter != change_map->end(); ++iter) {
        json_map row;
        change_descriptor fields = iter->second;
        if (!fields.oper_complete) {
            continue;
        }
        row["fidstr"] = iter->first;
        row["parent_fidstr"] = fields.parent_fidstr;
        row["object_type"] = object_type_to_str(fields.object_type);
        row["object_name"] = fields.object_name;
        row["lustre_path"] = fields.lustre_path;
        row["event_type"] = event_type_to_str(fields.last_event);
        row["file_size"] = fields.file_size;
        rows.push_back(row);
    }

    change_map_json["change_records"] = rows;
    change_map_json["lustre_root_path"] = lustre_root_path;
    change_map_json["register_path"] = register_path;
    change_map_json["resource_id"] = resource_id;

    //std::stringstream ss;
    //ss << change_map_json;
    std::string const json_str = change_map_json.to_string();

    if (json_str.length() > buffer_size-1) {
        // error
    }
    snprintf(buffer, buffer_size, "%s", json_str.c_str());
}
    

}



std::map<std::string, change_descriptor> *change_map = NULL;
std::map<std::string, change_descriptor> *get_change_map_instance() {
    if (!change_map)
        change_map = new std::map<std::string, change_descriptor>();
    return change_map;
}

std::string event_type_to_str(create_delete_event_type_enum type) {
    switch (type) {
        case OTHER:
            return "OTHER";
            break;
        case CREAT:
            return "CREAT";
            break;
        case UNLINK:
            return "UNLINK";
            break;
        case RMDIR:
            return "RMDIR";
            break;
        case MKDIR:
            return "MKDIR";
            break;
        case RENAME:
            return "RENAME";
            break;
    }
    return "";
}

std::string object_type_to_str(object_type_enum type) {
    switch (type) {
        case _FILE:
            return "FILE";
            break;
        case _DIR:
            return "DIR";
            break;
    }
    return "";
}


void remove_fidstr_from_table(const char *fidstr_cstr) {
    std::string fidstr(fidstr_cstr);
    get_change_map_instance()->erase(fidstr);
}

