#include <jeayeson/jeayeson.hpp>
#include <iostream>
#include <sstream>
#include <typeinfo>
#include <string>

#include "config.h"
#include "logging.hpp"

char mdtname[MAX_CONFIG_VALUE_SIZE];
char lustre_root_path[MAX_CONFIG_VALUE_SIZE];
char register_path[MAX_CONFIG_VALUE_SIZE];
char resource_name[MAX_CONFIG_VALUE_SIZE];
int64_t resource_id;

FILE *dbgstream = stdout;
int  log_level = LOG_INFO;

#define LOG_FATAL    (1)
#define LOG_ERR      (2)
#define LOG_WARN     (3)
#define LOG_INFO     (4)
#define LOG_DBG      (5)



int read_key_from_map(const json_map& config_map, const std::string &key, std::string& value) {
    auto entry = config_map.find(key);
    if (entry == config_map.end()) {
       return -1;
    }
    std::stringstream tmp;
    tmp << entry->second;
    value = tmp.str();

    // remove quotes
    value.erase(remove(value.begin(), value.end(), '\"' ), value.end());
    return 0;
}

void set_log_level(std::string log_level_str) {
    if (log_level_str == "LOG_FATAL") 
        log_level = LOG_FATAL; 
    else if (log_level_str == "LOG_ERR")
        log_level = LOG_ERR;
    else if (log_level_str == "LOG_WARN")
        log_level = LOG_WARN;
    else if (log_level_str == "LOG_INFO")
        log_level = LOG_INFO;
    else if (log_level_str == "LOG_DBG")
        log_level = LOG_DBG;
}    

extern "C" {

int read_config_file(const char *filename) {

    std::string mdtname_str;
    std::string lustre_root_path_str;
    std::string register_path_str;
    std::string resource_name_str;
    std::string resource_id_str;
    std::string log_level_str;

    try {
        json_map config_map{ json_file{ filename } };

        if (read_key_from_map(config_map, "mdtname", mdtname_str) != 0) {
            LOG(LOG_ERR, "Key mdtname missing from %s\n", filename);
            return -1;
        }
        if (read_key_from_map(config_map, "lustre_root_path", lustre_root_path_str) != 0) {
            LOG(LOG_ERR, "Key lustre_root_path missing from %s\n", filename);
            return -1;
        }
        if (read_key_from_map(config_map, "register_path", register_path_str) != 0) {
            LOG(LOG_ERR, "Key register_path missing from %s\n", filename);
            return -1;
        }
        if (read_key_from_map(config_map, "resource_name", resource_name_str) != 0) {
            LOG(LOG_ERR, "Key resource_name missing from %s\n", filename);
            return -1;
        }
        if (read_key_from_map(config_map, "resource_id", resource_id_str) != 0) {
            LOG(LOG_ERR, "Key resource_id missing from %s\n", filename);
            return -1;
        }

        // populate global variables
        snprintf(mdtname, MAX_CONFIG_VALUE_SIZE, "%s", mdtname_str.c_str());
        snprintf(lustre_root_path, MAX_CONFIG_VALUE_SIZE, "%s", lustre_root_path_str.c_str());
        snprintf(register_path, MAX_CONFIG_VALUE_SIZE, "%s", register_path_str.c_str());
        snprintf(resource_name, MAX_CONFIG_VALUE_SIZE, "%s", mdtname_str.c_str());

        if (read_key_from_map(config_map, "log_level", log_level_str) == 0) {
            printf("log_level_str = %s\n", log_level_str.c_str());
            set_log_level(log_level_str);
        } 

        printf("log level set to %i\n", log_level);


        try {
            resource_id = std::stoi(resource_id_str);
        } catch (std::invalid_argument& e) {
            LOG(LOG_ERR, "Could not parse resource_id as an integer.\n");
            return -1;
        }

    } catch (std::exception& e) {
        LOG(LOG_ERR, "Could not read %s - %s\n", filename, e.what());
        return -1;
    }
    return 0;

}

}
