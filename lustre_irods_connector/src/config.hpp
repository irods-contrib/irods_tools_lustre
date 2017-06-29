#ifndef _IRODS_LUSTRE_CHANGELOG_CONFIG_H
#define _IRODS_LUSTRE_CHANGELOG_CONFIG_H

const int MAX_CONFIG_VALUE_SIZE = 256;

typedef struct lustre_irods_connector_cfg {
    char mdtname[MAX_CONFIG_VALUE_SIZE];
    char lustre_root_path[MAX_CONFIG_VALUE_SIZE];
    char irods_register_path[MAX_CONFIG_VALUE_SIZE];
    char irods_resource_name[MAX_CONFIG_VALUE_SIZE];
    int64_t irods_resource_id;
    unsigned int changelog_poll_interval_seconds;
    unsigned int update_irods_interval_seconds;
    unsigned int changelog_reader_recv_port;
    unsigned int irods_client_recv_port;
} lustre_irods_connector_cfg_t;


int read_config_file(const char *filename, lustre_irods_connector_cfg_t *config_struct);

#endif
