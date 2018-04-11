#include <jeayeson/jeayeson.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/format.hpp>
#include <iostream>
#include <sstream>
#include <typeinfo>
#include <string>
#include <algorithm>

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

void set_log_level(const std::string& log_level_str) {
    if ("LOG_FATAL" == log_level_str) {
        log_level = LOG_FATAL; 
    } else if ("LOG_ERR" == log_level_str) {
        log_level = LOG_ERR;
    } else if ("LOG_WARN" == log_level_str) {
        log_level = LOG_WARN;
    } else if ("LOG_INFO" == log_level_str) {
        log_level = LOG_INFO;
    } else if ("LOG_DBG" == log_level_str) {
        log_level = LOG_DBG;
    }
}    

int read_config_file(const std::string& filename, lustre_irods_connector_cfg_t *config_struct) {

    if ("" == filename) {
        LOG(LOG_ERR, "read_config_file did not receive a filename\n");
        return lustre_irods::CONFIGURATION_ERROR;
    }

    if (nullptr == config_struct) {
        LOG(LOG_ERR, "Null config_struct sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    std::string log_level_str;
    std::string changelog_poll_interval_seconds_str;
    std::string irods_client_connect_failure_retry_seconds_str;
    std::string irods_updater_thread_count_str;
    std::string maximum_records_per_update_to_irods_str;
    std::string maximum_records_to_receive_from_lustre_changelog_str;
    std::string message_receive_timeout_msec_str;

    try {
        json_map config_map{ json_file{ filename.c_str() } };

        if (0 != read_key_from_map(config_map, "mdtname", config_struct->mdtname)) {
            LOG(LOG_ERR, "Key mdtname missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "lustre_root_path", config_struct->lustre_root_path)) {
            LOG(LOG_ERR, "Key lustre_root_path missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "irods_register_path", config_struct->irods_register_path)) {
            LOG(LOG_ERR, "Key register_path missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "irods_resource_name", config_struct->irods_resource_name)) {
            LOG(LOG_ERR, "Key resource_name missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "irods_api_update_type", config_struct->irods_api_update_type)) {
            std::transform(config_struct->irods_api_update_type.begin(), config_struct->irods_api_update_type.end(), 
                    config_struct->irods_api_update_type.begin(), ::tolower);
            LOG(LOG_ERR, "Key irods_api_update_type missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "changelog_poll_interval_seconds", changelog_poll_interval_seconds_str)) {
            LOG(LOG_ERR, "Key changelog_poll_interval_seconds missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "irods_client_connect_failure_retry_seconds", irods_client_connect_failure_retry_seconds_str)) {
            LOG(LOG_ERR, "Key irods_client_connect_failure_retry_seconds missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "irods_client_broadcast_address", config_struct->irods_client_broadcast_address)) {
            LOG(LOG_ERR, "Key changelog_reader_recv_port missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "changelog_reader_broadcast_address", config_struct->changelog_reader_broadcast_address)) {
            LOG(LOG_ERR, "Key changelog_reader_broadcast_address missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "changelog_reader_push_work_address", config_struct->changelog_reader_push_work_address)) {
            LOG(LOG_ERR, "Key changelog_reader_push_work_address missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "result_accumulator_push_address", config_struct->result_accumulator_push_address)) {
            LOG(LOG_ERR, "Key result_accumulator_push_address missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }

        if (0 != read_key_from_map(config_map, "irods_updater_thread_count", irods_updater_thread_count_str)) {
            LOG(LOG_ERR, "Key irods_updater_thread_count missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "maximum_records_per_update_to_irods", maximum_records_per_update_to_irods_str)) {
            LOG(LOG_ERR, "Key num_records_per_update_to_irods missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "maximum_records_to_receive_from_lustre_changelog", maximum_records_to_receive_from_lustre_changelog_str)) {
            LOG(LOG_ERR, "Key num_records_per_update_to_irods missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "message_receive_timeout_msec", message_receive_timeout_msec_str)) {
            LOG(LOG_ERR, "Key message_receive_timeout_msec missing from %s\n", filename.c_str());
            return lustre_irods::CONFIGURATION_ERROR;
        }



        // populate config variables

        if (0 == read_key_from_map(config_map, "log_level", log_level_str)) {
            printf("log_level_str = %s\n", log_level_str.c_str());
            set_log_level(log_level_str);
        } 

        printf("log level set to %i\n", log_level);

        // convert irods_api_update_type to lowercase 
        std::transform(config_struct->irods_api_update_type.begin(), config_struct->irods_api_update_type.end(), 
                 config_struct->irods_api_update_type.begin(), ::tolower);

        // error if the setting is not "direct" or "policy"
        if (config_struct->irods_api_update_type != "direct" && config_struct->irods_api_update_type != "policy") { 
            LOG(LOG_ERR, "Could not parse irods_api_update_type.  It must be either \"direct\" or \"policy\".\n");
            return lustre_irods::CONFIGURATION_ERROR;
        }

        try {
            config_struct->changelog_poll_interval_seconds = boost::lexical_cast<unsigned int>(changelog_poll_interval_seconds_str);
        } catch (boost::bad_lexical_cast& e) {
            LOG(LOG_ERR, "Could not parse changelog_poll_interval_seconds as an integer.\n");
            return lustre_irods::CONFIGURATION_ERROR;
        }

        try {
            config_struct->irods_client_connect_failure_retry_seconds = boost::lexical_cast<unsigned int>(irods_client_connect_failure_retry_seconds_str);
        } catch (boost::bad_lexical_cast& e) {
            LOG(LOG_ERR, "Could not parse irods_client_connect_failure_retry_seconds as an integer.\n");
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

        try {
            config_struct->maximum_records_to_receive_from_lustre_changelog = boost::lexical_cast<unsigned int>(maximum_records_to_receive_from_lustre_changelog_str);
        } catch (boost::bad_lexical_cast& e) {
            LOG(LOG_ERR, "Could not parse maximum_records_to_receive_from_lustre_changelog as an integer.\n");
            return lustre_irods::CONFIGURATION_ERROR;
        }


        try {
            config_struct->message_receive_timeout_msec = boost::lexical_cast<unsigned int>(message_receive_timeout_msec_str);
        } catch (boost::bad_lexical_cast& e) {
            LOG(LOG_ERR, "Could not parse message_receive_timeout_msec as an integer.\n");
            return lustre_irods::CONFIGURATION_ERROR;
        }


        // read individual thread connection parameters
        boost::format format_object("thread_%i_connection_parameters");
        for (unsigned int i = 0; i < config_struct->irods_updater_thread_count; ++i) {


            std::string key = str(format_object % i); 

            auto entry = config_map.find(key);
            if (config_map.end() != entry) {
                irods_connection_cfg_t config_entry;
                auto tmp = entry->second["irods_host"];
                std::stringstream ss;
                ss << tmp;
                std::string value = ss.str();
                value.erase(remove(value.begin(), value.end(), '\"' ), value.end());
                if ("null" == value) {
                    LOG(LOG_ERR, "Could not read irods_host for connection %u.  Either define it or leave off connection 1 paramters to use defaults from the iRODS environment.\n", i);
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

    } catch (std::exception& e) {
        LOG(LOG_ERR, "Could not read %s - %s\n", filename.c_str(), e.what());
        return lustre_irods::CONFIGURATION_ERROR;
    }
    return lustre_irods::SUCCESS;

}

