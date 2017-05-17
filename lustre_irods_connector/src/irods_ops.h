#ifndef __irods_ops_h
#define __irods_ops_h

#include "rodsType.h"
//#include "../../irods_lustre_api/src/inout_structs.h"
#include "inout_structs.h"

int send_change_map_to_irods(irodsLustreApiInp_t *inp);

#ifdef __cplusplus
extern "C" {
#endif

int instantiate_irods_connection();
void disconnect_irods_connection();

#ifdef __cplusplus
}
#endif

#endif
