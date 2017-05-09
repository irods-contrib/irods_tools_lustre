#include <jeayeson/jeayeson.hpp>
#include <iostream>
#include <sstream>
#include <typeinfo>
#include <string>

#include "changelog_config.h"

char mdtname[MAX_CONFIG_VALUE_SIZE];
char lustre_root_path[MAX_CONFIG_VALUE_SIZE];
char register_path[MAX_CONFIG_VALUE_SIZE];
char resource_name[MAX_CONFIG_VALUE_SIZE];
int64_t resource_id;


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

extern "C" {

int read_config_file(const char *filename) {

    std::string mdtname_str;
    std::string lustre_root_path_str;
    std::string register_path_str;
    std::string resource_name_str;
    std::string resource_id_str;

    try {
        json_map config_map{ json_file{ filename } };

        if (read_key_from_map(config_map, "mdtname", mdtname_str) != 0) {
            std::cerr << "Key mdtname missing from irods_lustre_config.json" << std::endl;
            return -1;
        }
        if (read_key_from_map(config_map, "lustre_root_path", lustre_root_path_str) != 0) {
            std::cerr << "Key lustre_root_path missing from irods_lustre_config.json" << std::endl;
            return -1;
        }
        if (read_key_from_map(config_map, "register_path", register_path_str) != 0) {
            std::cerr << "Key register_path missing from irods_lustre_config.json" << std::endl;
            return -1;
        }
        if (read_key_from_map(config_map, "resource_name", resource_name_str) != 0) {
            std::cerr << "Key resource_name missing from irods_lustre_config.json" << std::endl;
            return -1;
        }
        if (read_key_from_map(config_map, "resource_id", resource_id_str) != 0) {
            std::cerr << "Key resource_id missing from irods_lustre_config.json" << std::endl;
            return -1;
        }

        // populate global variables
        snprintf(mdtname, MAX_CONFIG_VALUE_SIZE, "%s", mdtname_str.c_str());
        snprintf(lustre_root_path, MAX_CONFIG_VALUE_SIZE, "%s", lustre_root_path_str.c_str());
        snprintf(register_path, MAX_CONFIG_VALUE_SIZE, "%s", register_path_str.c_str());
        snprintf(resource_name, MAX_CONFIG_VALUE_SIZE, "%s", mdtname_str.c_str());
        try {
            resource_id = std::stoi(resource_id_str);
        } catch (std::invalid_argument& e) {
            std::cerr << "Could not parse resource_id as an integer." << std::endl;
            return -1;
        }

    } catch (std::exception& e) {
        std::cout << "Could not read irods_lustre_config.json" << std::endl;
        std::cout << e.what() << std::endl;
        return -1;
    }
    return 0;

}

}
