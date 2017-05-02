#include <map>
#include <string>
#include <ctime>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

// boost headers
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>

#include "lustre_change_table.hpp"

extern "C" {


void lustre_close(const char *fidstr, const char *lustre_path) {
    irods_lustre::lustre_change_table::instance()->handle_close(std::string(fidstr), std::string(lustre_path)); 
} 

void lustre_mkdir(const char *fidstr, const char *lustre_path) {
    irods_lustre::lustre_change_table::instance()->handle_mkdir(std::string(fidstr), std::string(lustre_path));
}

void lustre_rmdir(const char *fidstr, const char *lustre_path) {
    irods_lustre::lustre_change_table::instance()->handle_rmdir(std::string(fidstr), std::string(lustre_path));
}

void lustre_unlink(const char *fidstr, const char *lustre_path) {
    irods_lustre::lustre_change_table::instance()->handle_unlink(std::string(fidstr), std::string(lustre_path));
}

void lustre_rename(const char *fidstr, const char *old_lustre_path, char *new_lustre_path) {
    irods_lustre::lustre_change_table::instance()->handle_rename(std::string(fidstr), std::string(old_lustre_path), std::string(new_lustre_path));
}

void lustre_create(const char *fidstr, const char *lustre_path) {
    irods_lustre::lustre_change_table::instance()->handle_create(std::string(fidstr), std::string(lustre_path));
}

void lustre_mtime(const char *fidstr, const char *lustre_path) {
    irods_lustre::lustre_change_table::instance()->handle_mtime(std::string(fidstr), std::string(lustre_path));
}

void lustre_trunc(const char *fidstr, const char *lustre_path) {
    irods_lustre::lustre_change_table::instance()->handle_trunc(std::string(fidstr), std::string(lustre_path));
}

void lustre_print_change_table() {
    irods_lustre::lustre_change_table::instance()->print_table();
}

}

namespace irods_lustre {

lustre_change_table *_instance = NULL;

std::string event_type_to_str(create_delete_event_type type) {
    switch (type) {
        case NA:
            return "NA";
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

lustre_change_table *lustre_change_table::instance() {

    if (!_instance)
        _instance = new lustre_change_table();
    return _instance;
}

lustre_change_table::lustre_change_table(lustre_change_table const&) {}
lustre_change_table& lustre_change_table::operator=(lustre_change_table const&) { return *_instance; }


void lustre_change_table::handle_close(const std::string& fidstr, const std::string& lustre_path) {
    auto iter = change_map.find(fidstr);
    if(iter != change_map.end()) {
        change_descriptor& entry = iter->second;
        entry.oper_complete = true;
        entry.timestamp = time(NULL);
    } else {
        // this is probably an append so no file update is done
        change_descriptor entry;
        entry.lustre_path = lustre_path;
        entry.oper_complete = true;
        entry.timestamp = time(NULL);
        change_map[fidstr] = entry;
    }
}

void lustre_change_table::handle_mkdir(const std::string& fidstr, const std::string& lustre_path) {
    auto iter = change_map.find(fidstr);
    if(iter != change_map.end()) {
        change_descriptor& entry = iter->second;
        entry.oper_complete = true;
        entry.last_event = MKDIR;
        entry.timestamp = time(NULL);
    } else {
        change_descriptor entry;
        entry.lustre_path = lustre_path;
        entry.oper_complete = true;
        entry.last_event = MKDIR;
        entry.timestamp = time(NULL);
        change_map[fidstr] = entry;
    }
}

void lustre_change_table::handle_rmdir(const std::string& fidstr, const std::string& lustre_path) {
    auto iter = change_map.find(fidstr);
    if(iter != change_map.end()) {
        change_descriptor& entry = iter->second;
        entry.lustre_path = lustre_path;
        entry.oper_complete = true;
        entry.last_event = RMDIR;
        entry.timestamp = time(NULL);
    } else {
        change_descriptor entry;
        entry.oper_complete = true;
        entry.last_event = RMDIR;
        entry.timestamp = time(NULL);
        change_map[fidstr] = entry;
    }
}

void lustre_change_table::handle_unlink(const std::string& fidstr, const std::string& lustre_path) {
    auto iter = change_map.find(fidstr);
    if(iter != change_map.end()) {   
        change_descriptor& entry = iter->second;
        entry.oper_complete = true;
        entry.last_event = UNLINK;
        entry.timestamp = time(NULL);
    } else {
        change_descriptor entry;
        entry.lustre_path = lustre_path;
        entry.oper_complete = true;
        entry.last_event = UNLINK;
        entry.timestamp = time(NULL);
        change_map[fidstr] = entry;
    }

}

void lustre_change_table::handle_rename(const std::string& fidstr, const std::string& old_path, std::string new_path) {
    // TODO look at this
    auto iter = change_map.find(fidstr);
    std::string original_path;

    // if there is a previous entry, just update the lustre_path to the new path
    // otherwise, add a new entry
    if(iter != change_map.end()) {
        iter->second.lustre_path = new_path;
 
    } else {

        change_descriptor entry;
        entry.lustre_path = new_path;
        entry.oper_complete = true;
        entry.last_event = RENAME;
        entry.timestamp = time(NULL);
        entry.original_path = original_path;
        change_map[fidstr] = entry;
    }

    // see if this is a file or directory
    struct stat statbuf;
    if (stat(new_path.c_str(), &statbuf) == 0 && S_ISDIR(statbuf.st_mode)) {
        // search through and update all references in table
        for (auto iter = change_map.begin(); iter != change_map.end(); ++iter) {
            std::string p1 = iter->second.original_path; 
            std::string p2 = iter->second.lustre_path;

            if (p1.length() != old_path.length() && boost::starts_with(p1, old_path)) {
               printf("!!!! Need to update %s !!!!\n", p1.c_str());
               iter->second.original_path.replace(0, old_path.length(), new_path); 

            }

            if (p2.length() != old_path.length() && boost::starts_with(p2, old_path)) {
               printf("!!!! Need to update %s !!!!\n", p2.c_str());
               iter->second.lustre_path.replace(1, old_path.length(), new_path);
            }
        } 
    }

}

void lustre_change_table::handle_create(const std::string& fidstr, const std::string& lustre_path) {
    auto iter = change_map.find(fidstr);
    if(iter != change_map.end()) {
        change_descriptor& entry = iter->second;
        entry.oper_complete = false;
        entry.last_event = CREAT;
        entry.timestamp = time(NULL);
    } else {
        change_descriptor entry;
        entry.lustre_path = lustre_path;
        entry.oper_complete = false;
        entry.last_event = CREAT;
        entry.timestamp = time(NULL);
        change_map[fidstr] = entry;
    }
}

void lustre_change_table::handle_mtime(const std::string& fidstr, const std::string& lustre_path) {
    auto iter = change_map.find(fidstr);
    if(iter != change_map.end()) {   
        change_descriptor& entry = iter->second;
        entry.oper_complete = false;
        entry.timestamp = time(NULL);
    } else {
        change_descriptor entry;
        entry.lustre_path = lustre_path;
        entry.oper_complete = false;
        entry.timestamp = time(NULL);
        change_map[fidstr] = entry;
    }

}

void lustre_change_table::handle_trunc(const std::string& fidstr, const std::string& lustre_path) {
    auto iter = change_map.find(fidstr);
    if(iter != change_map.end()) {
        change_descriptor& entry = iter->second;
        entry.oper_complete = false;
        entry.timestamp = time(NULL);
    } else {
        change_descriptor entry;
        entry.lustre_path = lustre_path;
        entry.oper_complete = false;
        entry.timestamp = time(NULL);
        change_map[fidstr] = entry;
    }
}

void lustre_change_table::print_table() {
    char time_str[18];
    printf("\n");
    printf("%-30s %-30s %-17s %-11s %-15s %-30s\n", "FIDSTR", "LUSTRE_PATH", "TIME", "EVENT_TYPE", "OPER_COMPLETE?", "ORIGINAL_PATH");
    printf("%-30s %-30s %-17s %-11s %-15s %-30s\n", "------", "-----------", "----", "----------", "--------------", "-------------");
    for (auto iter = change_map.begin(); iter != change_map.end(); ++iter) {
         std::string fidstr = iter->first;
         change_descriptor fields = iter->second;

         struct tm *timeinfo;
         timeinfo = localtime(&fields.timestamp);
         strftime(time_str, sizeof(time_str), "%Y%m%d %I:%M:%S", timeinfo);

         printf("%-30s %-30s %-17s %-11s %-15s %-30s\n", fidstr.c_str(), fields.lustre_path.c_str(), time_str, event_type_to_str(fields.last_event).c_str(), 
                 fields.oper_complete == 1 ? "true" : "false", fields.original_path.c_str());
    }
}

       
} 

