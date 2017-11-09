#include <jeayeson/jeayeson.hpp>
#include <boost/lexical_cast.hpp>
#include <iostream>
#include <sstream>
#include <typeinfo>
#include <string>

#include "config.hpp"
#include "logging.hpp"
#include "lustre_irods_errors.hpp"

FILE *dbgstream = stdout;
int  log_level = LOG_INFO;

int read_key_from_map(const json_map& config_map, const std::string &key, std::string& value) {
    auto entry = config_map.find(key);
    if (entry == config_map.end()) {
       LOG(LOG_ERR, "Could not find key %s in configuration\n", key.c_str());
       return lustre_irods::CONFIGURATION_ERROR;
    }
    std::stringstream tmp;
    tmp << entry->second;
    value = tmp.str();

    // remove quotes
    value.erase(remove(value.begin(), value.end(), '\"' ), value.end());
    return lustre_irods::SUCCESS;
}

void set_log_level(std::string log_level_str) {
    if (log_level_str == "LOG_FATAL") {
        log_level = LOG_FATAL; 
    } else if (log_level_str == "LOG_ERR") {
        log_level = LOG_ERR;
    } else if (log_level_str == "LOG_WARN") {
        log_level = LOG_WARN;
    } else if (log_level_str == "LOG_INFO") {
        log_level = LOG_INFO;
    } else if (log_level_str == "LOG_DBG") {
        log_level = LOG_DBG;
    }
}    

int read_config_file(const char *filename, lustre_irods_connector_cfg_t *config_struct) {

    if (filename == nullptr) {
        LOG(LOG_ERR, "read_config_file did not receive a filename\n");
        return lustre_irods::CONFIGURATION_ERROR;
    }

    if (config_struct == nullptr) {
        LOG(LOG_ERR, "Null config_struct sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    std::string mdtname_str;
    std::string lustre_root_path_str;
    std::string irods_register_path_str;
    std::string irods_resource_name_str;
    std::string log_level_str;
    std::string changelog_poll_interval_seconds_str;
    std::string update_irods_interval_seconds_str;
    std::string irods_client_ipc_address_str;
    std::string changelog_reader_ipc_address_str;
    std::string irods_updater_thread_count_str;

    try {
        json_map config_map{ json_file{ filename } };

        if (read_key_from_map(config_map, "mdtname", mdtname_str) != 0) {
            LOG(LOG_ERR, "Key mdtname missing from %s\n", filename);
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (read_key_from_map(config_map, "lustre_root_path", lustre_root_path_str) != 0) {
            LOG(LOG_ERR, "Key lustre_root_path missing from %s\n", filename);
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (read_key_from_map(config_map, "irods_register_path", irods_register_path_str) != 0) {
            LOG(LOG_ERR, "Key register_path missing from %s\n", filename);
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (read_key_from_map(config_map, "irods_resource_name", irods_resource_name_str) != 0) {
            LOG(LOG_ERR, "Key resource_name missing from %s\n", filename);
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (read_key_from_map(config_map, "changelog_poll_interval_seconds", changelog_poll_interval_seconds_str) != 0) {
            LOG(LOG_ERR, "Key changelog_poll_interval_seconds missing from %s\n", filename);
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (read_key_from_map(config_map, "update_irods_interval_seconds", update_irods_interval_seconds_str) != 0) {
            LOG(LOG_ERR, "Key update_irods_interval_seconds missing from %s\n", filename);
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (read_key_from_map(config_map, "irods_client_ipc_address", irods_client_ipc_address_str) != 0) {
            LOG(LOG_ERR, "Key changelog_reader_recv_port missing from %s\n", filename);
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (read_key_from_map(config_map, "changelog_reader_ipc_address", changelog_reader_ipc_address_str) != 0) {
            LOG(LOG_ERR, "Key update_irods_interval_seconds missing from %s\n", filename);
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (read_key_from_map(config_map, "irods_updater_thread_count", irods_updater_thread_count_str) != 0) {
            LOG(LOG_ERR, "Key irods_updater_thread_count missing from %s\n", filename);
            return lustre_irods::CONFIGURATION_ERROR;
        }
 


        // populate config variables
        snprintf(config_struct->mdtname, MAX_CONFIG_VALUE_SIZE, "%s", mdtname_str.c_str());
        snprintf(config_struct->lustre_root_path, MAX_CONFIG_VALUE_SIZE, "%s", lustre_root_path_str.c_str());
        snprintf(config_struct->irods_register_path, MAX_CONFIG_VALUE_SIZE, "%s", irods_register_path_str.c_str());
        snprintf(config_struct->irods_resource_name, MAX_CONFIG_VALUE_SIZE, "%s", irods_resource_name_str.c_str());

        if (read_key_from_map(config_map, "log_level", log_level_str) == 0) {
            printf("log_level_str = %s\n", log_level_str.c_str());
            set_log_level(log_level_str);
        } 

        printf("log level set to %i\n", log_level);


        try {
            config_struct->changelog_poll_interval_seconds = boost::lexical_cast<unsigned int>(changelog_poll_interval_seconds_str);
        } catch (boost::bad_lexical_cast& e) {
            LOG(LOG_ERR, "Could not parse changelog_poll_interval_seconds as an integer.\n");
            return lustre_irods::CONFIGURATION_ERROR;
        }

        try {
            config_struct->update_irods_interval_seconds = boost::lexical_cast<unsigned int>(update_irods_interval_seconds_str);
        } catch (boost::bad_lexical_cast& e) {
            LOG(LOG_ERR, "Could not parse update_irods_interval_seconds as an integer.\n");
            return lustre_irods::CONFIGURATION_ERROR;
        }

        snprintf(config_struct->irods_client_ipc_address, MAX_CONFIG_VALUE_SIZE, "%s", irods_client_ipc_address_str.c_str());
        snprintf(config_struct->changelog_reader_ipc_address, MAX_CONFIG_VALUE_SIZE, "%s", changelog_reader_ipc_address_str.c_str());

        try {
            config_struct->irods_updater_thread_count = boost::lexical_cast<unsigned int>(irods_updater_thread_count_str);
        } catch (boost::bad_lexical_cast& e) {
            LOG(LOG_ERR, "Could not parse irods_updater_thread_count as an integer.\n");
            return lustre_irods::CONFIGURATION_ERROR;
        }



        /*try {
            config_struct->changelog_reader_connection = boost::lexical_cast<unsigned int>(changelog_reader_recv_port_str);
        } catch (boost::bad_lexical_cast& e) {
            LOG(LOG_ERR, "Could not parse changelog_reader_recv_port as an integer.\n");
            return lustre_irods::CONFIGURATION_ERROR;
        }

        try {
            config_struct->irods_client_recv_port = boost::lexical_cast(irods_client_recv_port_str);
        } catch (boost::bad_lexical_cast& e) {
            LOG(LOG_ERR, "Could not parse irods_client_recv_port as an integer.\n");
            return lustre_irods::CONFIGURATION_ERROR;
        }*/

    } catch (std::exception& e) {
        LOG(LOG_ERR, "Could not read %s - %s\n", filename, e.what());
        return lustre_irods::CONFIGURATION_ERROR;
    }
    return lustre_irods::SUCCESS;

}

