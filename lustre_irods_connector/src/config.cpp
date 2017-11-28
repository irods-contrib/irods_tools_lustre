#include <jeayeson/jeayeson.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/format.hpp>
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

int read_config_file(const std::string& filename, lustre_irods_connector_cfg_t *config_struct) {

    if (filename == "") {
        LOG(LOG_ERR, "read_config_file did not receive a filename\n");
        return lustre_irods::CONFIGURATION_ERROR;
    }

    if (config_struct == nullptr) {
        LOG(LOG_ERR, "Null config_struct sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    //std::string mdtname_str;
    //std::string lustre_root_path_str;
    //std::string irods_register_path_str;
    //std::string irods_resource_name_str;
    std::string log_level_str;
    std::string changelog_poll_interval_seconds_str;
    //std::string irods_client_broadcast_address_str;
    //td::string changelog_reader_broadcast_address_str;
    //std::string changelog_reader_push_work_address_str;
    //std::string result_accumulator_push_address_str;
    std::string irods_updater_thread_count_str;
    std::string maximum_records_per_update_to_irods_str;

    try {
        json_map config_map{ json_file{ filename.c_str() } };

        if (read_key_from_map(config_map, "mdtname", config_struct->mdtname) != 0) {
            LOG(LOG_ERR, "Key mdtname missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (read_key_from_map(config_map, "lustre_root_path", config_struct->lustre_root_path) != 0) {
            LOG(LOG_ERR, "Key lustre_root_path missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (read_key_from_map(config_map, "irods_register_path", config_struct->irods_register_path) != 0) {
            LOG(LOG_ERR, "Key register_path missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (read_key_from_map(config_map, "irods_resource_name", config_struct->irods_resource_name) != 0) {
            LOG(LOG_ERR, "Key resource_name missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (read_key_from_map(config_map, "changelog_poll_interval_seconds", changelog_poll_interval_seconds_str) != 0) {
            LOG(LOG_ERR, "Key changelog_poll_interval_seconds missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (read_key_from_map(config_map, "irods_client_broadcast_address", config_struct->irods_client_broadcast_address) != 0) {
            LOG(LOG_ERR, "Key changelog_reader_recv_port missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (read_key_from_map(config_map, "changelog_reader_broadcast_address", config_struct->changelog_reader_broadcast_address) != 0) {
            LOG(LOG_ERR, "Key changelog_reader_broadcast_address missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (read_key_from_map(config_map, "changelog_reader_push_work_address", config_struct->changelog_reader_push_work_address) != 0) {
            LOG(LOG_ERR, "Key changelog_reader_push_work_address missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (read_key_from_map(config_map, "result_accumulator_push_address", config_struct->result_accumulator_push_address) != 0) {
            LOG(LOG_ERR, "Key result_accumulator_push_address missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }

        if (read_key_from_map(config_map, "irods_updater_thread_count", irods_updater_thread_count_str) != 0) {
            LOG(LOG_ERR, "Key irods_updater_thread_count missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (read_key_from_map(config_map, "maximum_records_per_update_to_irods", maximum_records_per_update_to_irods_str) != 0) {
            LOG(LOG_ERR, "Key mum_records_per_update_to_irods missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }


        // populate config variables

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
            config_struct->irods_updater_thread_count = boost::lexical_cast<unsigned int>(irods_updater_thread_count_str);
        } catch (boost::bad_lexical_cast& e) {
            LOG(LOG_ERR, "Could not parse irods_updater_thread_count as an integer.\n");
            return lustre_irods::CONFIGURATION_ERROR;
        }

        try {
            config_struct->maximum_records_per_update_to_irods = boost::lexical_cast<unsigned int>(maximum_records_per_update_to_irods_str);
        } catch (boost::bad_lexical_cast& e) {
            LOG(LOG_ERR, "Could not parse maximum_records_per_update_to_irods as an integer.\n");
            return lustre_irods::CONFIGURATION_ERROR;
        }

        // read individual thread connection parameters
        boost::format format_object("thread_%i_connection_parameters");
        for (unsigned int i = 0; i < config_struct->irods_updater_thread_count; ++i) {


            std::string key = str(format_object % i); 

            auto entry = config_map.find(key);
            if (entry != config_map.end()) {
                irods_connection_cfg config_entry;
                auto tmp = entry->second["irods_host"];
                std::stringstream ss;
                ss << tmp;
                std::string value = ss.str();
                value.erase(remove(value.begin(), value.end(), '\"' ), value.end());
                if (value == "null") {
                    LOG(LOG_ERR, "Could not read irods_host for connection %d.  Either define it or leave off connection 1 paramters to use defaults from the iRODS environment.\n", i);
                    return lustre_irods::CONFIGURATION_ERROR;
                }

                config_entry.irods_host = value;

                // clear stringstream and read irods_port
                ss.str( std::string() );
                ss.clear();
                tmp = entry->second["irods_port"];
                ss << tmp;
                value = ss.str();
                value.erase(remove(value.begin(), value.end(), '\"' ), value.end());
                try {
                    config_entry.irods_port = boost::lexical_cast<unsigned int>(value);
                } catch (boost::bad_lexical_cast& e) {
                    LOG(LOG_ERR, "Could not parse port %s as an integer.\n", value.c_str());
                    return lustre_irods::CONFIGURATION_ERROR;
                }
                config_struct->irods_connection_list[i] = config_entry;

            }
        } 

        LOG(LOG_DBG, "  --- irods_connection_list --- \n");
        for (auto iter = config_struct->irods_connection_list.begin(); iter != config_struct->irods_connection_list.end(); ++iter) {
            LOG(LOG_DBG, "connection: %d\n", iter->first);
            LOG(LOG_DBG, "host: %s\n", iter->second.irods_host.c_str());
            LOG(LOG_DBG, "port: %d\n", iter->second.irods_port);
        }
        LOG(LOG_DBG, "  ----------------------------- \n");

    } catch (std::exception& e) {
        LOG(LOG_ERR, "Could not read %s - %s\n", filename.c_str(), e.what());
        return lustre_irods::CONFIGURATION_ERROR;
    }
    return lustre_irods::SUCCESS;

}

