#ifndef _IRODS_LUSTRE_CHANGELOG_CONFIG_H
#define _IRODS_LUSTRE_CHANGELOG_CONFIG_H

#include <vector>

const int MAX_CONFIG_VALUE_SIZE = 256;

typedef struct irods_connection_cfg {
    char irods_host[MAX_CONFIG_VALUE_SIZE];
    unsigned int irods_port;
    char irods_user[MAX_CONFIG_VALUE_SIZE];
} irods_connection_cfg_t;

typedef struct lustre_irods_connector_cfg {
    char mdtname[MAX_CONFIG_VALUE_SIZE];
    char lustre_root_path[MAX_CONFIG_VALUE_SIZE];
    char irods_register_path[MAX_CONFIG_VALUE_SIZE];
    char irods_resource_name[MAX_CONFIG_VALUE_SIZE];
    int64_t irods_resource_id;
    unsigned int changelog_poll_interval_seconds;
    char irods_client_broadcast_address[MAX_CONFIG_VALUE_SIZE];
    char changelog_reader_broadcast_address[MAX_CONFIG_VALUE_SIZE];
    char changelog_reader_push_work_address[MAX_CONFIG_VALUE_SIZE];
    char result_accumulator_push_address[MAX_CONFIG_VALUE_SIZE];
    unsigned int irods_updater_thread_count;
    unsigned int maximum_records_per_update_to_irods;
    std::vector<irods_connection_cfg_t> irods_connection_list;

} lustre_irods_connector_cfg_t;


int read_config_file(const char *filename, lustre_irods_connector_cfg_t *config_struct);

#endif
