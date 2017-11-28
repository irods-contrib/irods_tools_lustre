// irods includes
#include "rodsClient.h"
#include "parseCommandLine.h"
#include "rodsPath.h"
#include "regUtil.h"
#include "irods_client_api_table.hpp"
#include "irods_pack_table.hpp"
#include "rodsType.h"
#include "dataObjRename.h"
#include "rodsPath.h"
#include "lsUtil.h"
#include "irods_buffer_encryption.hpp"
#include "genQuery.h"

// local includes
//#include "../../irods_lustre_api/src/inout_structs.h"
#include "inout_structs.h"
#include "irods_ops.hpp"
#include "logging.hpp"
#include "config.hpp"
#include "lustre_irods_errors.hpp"

// other includes
#include <string>
#include <stdio.h>
#include <boost/filesystem.hpp>

lustre_irods_connection::~lustre_irods_connection() {
    printf("disconnecting irods\n");
    if (irods_conn) {
        rcDisconnect(irods_conn);
    }
    irods_conn = nullptr;    
}

int lustre_irods_connection::send_change_map_to_irods(irodsLustreApiInp_t *inp) const {


    LOG(LOG_DBG,"calling send_change_map_to_irods\n");

    if (inp == nullptr) {
        LOG(LOG_ERR, "Null inp sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }    


    if (!irods_conn) {
        LOG(LOG_ERR,"Error:  Called send_change_map_to_irods() without an active irods_conn\n");
        return lustre_irods::IRODS_CONNECTION_ERROR;
    }

    irods::pack_entry_table& pk_tbl = irods::get_pack_table();
    irods::api_entry_table& api_tbl = irods::get_client_api_table();
    init_api_table( api_tbl, pk_tbl );

    void *tmp_out = nullptr;
    LOG(LOG_DBG,"Before procApiRequest\n");
    int status = procApiRequest( irods_conn, 15001, inp, NULL,
                             &tmp_out, NULL );
    LOG(LOG_DBG,"After procApiRequest.  status=%i\n", status);

    int returnVal;

    if ( status < 0 ) {
        LOG(LOG_ERR, "\nERROR - failed to call our api - %i\n", status);
        returnVal = lustre_irods::IRODS_ERROR;
    } else {
        irodsLustreApiOut_t* out = static_cast<irodsLustreApiOut_t*>( tmp_out );
        LOG(LOG_INFO,"status is %i\n", out->status);
        returnVal = out->status;
    }

    free(tmp_out);
    return returnVal;
}

int lustre_irods_connection::populate_irods_resc_id(lustre_irods_connector_cfg_t *config_struct_ptr) {

    if (config_struct_ptr == nullptr) {
        LOG(LOG_ERR, "Null config_struct_ptr sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    if (!irods_conn) {
        LOG(LOG_ERR,"Error:  Called populate_irods_resc_id() without an active irods_conn\n");
        return lustre_irods::IRODS_CONNECTION_ERROR;
    }

    genQueryInp_t  gen_inp;
    genQueryOut_t* gen_out = NULL;
    memset(&gen_inp, 0, sizeof(gen_inp));

    char query_str[ MAX_NAME_LEN ];
    snprintf(query_str, MAX_NAME_LEN, "select RESC_ID where RESC_NAME = '%s'", config_struct_ptr->irods_resource_name.c_str());

    fillGenQueryInpFromStrCond(query_str, &gen_inp);
    gen_inp.maxRows = MAX_SQL_ROWS;

    int status = rcGenQuery(irods_conn, &gen_inp, &gen_out);

    if ( status < 0 || gen_out->rowCnt < 1) {
        if ( status == CAT_NO_ROWS_FOUND ) {
            LOG(LOG_ERR, "No resource found in iRODS for resc_name %s\n", config_struct_ptr->irods_resource_name.c_str());
            return lustre_irods::RESOURCE_NOT_FOUND_ERROR;
        }
        LOG(LOG_ERR, "Lookup resource id for resource %s returned error\n", config_struct_ptr->irods_resource_name.c_str());
        return lustre_irods::IRODS_ERROR;
    }

    sqlResult_t* resource_ids = getSqlResultByInx(gen_out, COL_R_RESC_ID);

    if (!resource_ids) {
        clearGenQueryInp(&gen_inp);
        freeGenQueryOut(&gen_out);
        LOG(LOG_ERR, "Error while translating resource name to resource id\n");
        return lustre_irods::RESOURCE_NOT_FOUND_ERROR;
    }

    try {
        config_struct_ptr->irods_resource_id = std::stoi(&(resource_ids->value[0]));
    } catch (std::invalid_argument& e) {
        clearGenQueryInp(&gen_inp);
        freeGenQueryOut(&gen_out);
        LOG(LOG_ERR, "Error translating resource id returned from iRODS to an integer.\n");
        return lustre_irods::INVALID_RESOURCE_ID_ERROR;
    }

    clearGenQueryInp(&gen_inp);
    freeGenQueryOut(&gen_out);
    return 0;
}

// Instantiate an iRODS connection.  If config_struct_ptr is null then the irods environment is used.  If config_struct_ptr is not
// null and there is an entry for this thread_number in config_struct_ptr->irods_connection_list then use the host and port from that.
// Otherwise use the irods environment for everything.
int lustre_irods_connection::instantiate_irods_connection(const lustre_irods_connector_cfg_t *config_struct_ptr, int thread_number) {

    rodsEnv myEnv;
    int status;
    rErrMsg_t errMsg;

    status = getRodsEnv( &myEnv );
    if (status < 0) {
        return lustre_irods::IRODS_ENVIRONMENT_ERROR;
    }

    std::string irods_host;
    int irods_port;
    if (config_struct_ptr != nullptr) {
        auto entry = config_struct_ptr->irods_connection_list.find(thread_number);
        if (entry != config_struct_ptr->irods_connection_list.end()) {
            irods_host = entry->second.irods_host;
            irods_port = entry->second.irods_port;
        } else {
            irods_host = myEnv.rodsHost;
            irods_port = myEnv.rodsPort;
        }
    } else {
        irods_host = myEnv.rodsHost;
        irods_port = myEnv.rodsPort;
    }

    LOG(LOG_ERR, "rcConnect being called.\n");
    irods_conn = rcConnect( irods_host.c_str(), irods_port, myEnv.rodsUserName, myEnv.rodsZone, 1, &errMsg );
    LOG(LOG_ERR, "irods_conn is %i\n", irods_conn != nullptr);

    if (irods_conn == NULL) {
        return lustre_irods::IRODS_CONNECTION_ERROR;
    }

    status = clientLogin(irods_conn);
    if (status != 0) {
        rcDisconnect(irods_conn);
        irods_conn = nullptr;
        LOG(LOG_ERR, "Error on clientLogin() - %i\n", status);
        return lustre_irods::IRODS_ERROR;
    }

    return 0;
}

