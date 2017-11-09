#ifndef __irods_ops_hpp
#define __irods_ops_hpp

#include "rodsType.h"
#include "inout_structs.h"
#include "config.hpp"
#include "rodsClient.h"

class lustre_irods_connection {
 public:
   unsigned int thread_number;
   rcComm_t *irods_conn;
   //lustre_irods_connection() : irods_conn(nullptr) {}
   lustre_irods_connection(unsigned int tnum) : thread_number(tnum), irods_conn(nullptr) {}
   ~lustre_irods_connection(); 
   int send_change_map_to_irods(irodsLustreApiInp_t *inp) const;
   int populate_irods_resc_id(lustre_irods_connector_cfg_t *config_struct_ptr);
   int instantiate_irods_connection();
};

#endif
