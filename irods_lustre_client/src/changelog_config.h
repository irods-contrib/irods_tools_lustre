#ifndef _IRODS_LUSTRE_CHANGELOG_CONFIG_H
#define _IRODS_LUSTRE_CHANGELOG_CONFIG_H

const int MAX_CONFIG_VALUE_SIZE = 256;

#ifdef __cplusplus
extern "C" {
#endif

int read_config_file(const char *filename);

#ifdef __cplusplus
}
#endif

#endif
