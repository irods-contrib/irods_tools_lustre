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

// other includes
#include <string>
#include <stdio.h>
#include <boost/filesystem.hpp>

// TODO change for multithreaded
//rcComm_t *irods_conn;

lustre_irods_connection::~lustre_irods_connection() {
    LOG(LOG_ERR, "disconnecting irods\n");
    if (irods_conn)
        rcDisconnect(irods_conn);
    irods_conn = nullptr;    
}

int lustre_irods_connection::send_change_map_to_irods(irodsLustreApiInp_t *inp) const {

    LOG(LOG_INFO,"calling send_change_map_to_irods\n");

    if (!irods_conn) {
        LOG(LOG_ERR,"Error:  Called send_change_map_to_irods() without an active irods_conn\n");
        return -1;
    }

    irods::pack_entry_table& pk_tbl = irods::get_pack_table();
    irods::api_entry_table& api_tbl = irods::get_client_api_table();
    init_api_table( api_tbl, pk_tbl );

    void *tmp_out = NULL;
    LOG(LOG_DBG,"Before procApiRequest\n");
    int status = procApiRequest( irods_conn, 15001, inp, NULL,
                             &tmp_out, NULL );
    LOG(LOG_DBG,"After procApiRequest.  status=%i\n", status);


    if ( status < 0 ) {
        LOG(LOG_ERR, "\n\nERROR - failed to call our api\n\n\n" );
        return status;
    } else {
        irodsLustreApiOut_t* out = static_cast<irodsLustreApiOut_t*>( tmp_out );
        LOG(LOG_INFO,"status is %i\n", out->status);
        return out->status;
    }
}

int lustre_irods_connection::populate_irods_resc_id(lustre_irods_connector_cfg_t *config_struct_ptr) {

    if (!irods_conn) {
        LOG(LOG_ERR,"Error:  Called populate_irods_resc_id() without an active irods_conn\n");
        return -1;
    }

    genQueryInp_t  gen_inp;
    genQueryOut_t* gen_out = NULL;
    memset(&gen_inp, 0, sizeof(gen_inp));

    char query_str[ MAX_NAME_LEN ];
    snprintf(query_str, MAX_NAME_LEN, "select RESC_ID where RESC_NAME = '%s'", config_struct_ptr->irods_resource_name);

    fillGenQueryInpFromStrCond(query_str, &gen_inp);
    gen_inp.maxRows = MAX_SQL_ROWS;

    int status = rcGenQuery(irods_conn, &gen_inp, &gen_out);

    if ( status < 0 || gen_out->rowCnt < 1) {
        if ( status == CAT_NO_ROWS_FOUND ) {
            LOG(LOG_ERR, "No resource found in iRODS for resc_name %s\n", config_struct_ptr->irods_resource_name);
        }
        else {
            LOG(LOG_ERR, "Lookup resource id for resource %s returned error\n", config_struct_ptr->irods_resource_name);
        }
        return -1;
    }

    sqlResult_t* resource_ids = getSqlResultByInx(gen_out, COL_R_RESC_ID);

    if (!resource_ids) {
        LOG(LOG_ERR, "Error while translating resource name to resource id\n");
        return -1;
    }

    try {
        config_struct_ptr->irods_resource_id = std::stoi(&(resource_ids->value[0]));
    } catch (std::invalid_argument& e) {
        LOG(LOG_ERR, "Error translating resource id returned from iRODS to an integer.\n");
        return -1;
    }

    return 0;
}

int lustre_irods_connection::instantiate_irods_connection() {

    rodsEnv myEnv;
    int status;
    rErrMsg_t errMsg;

    status = getRodsEnv( &myEnv );
    if (status < 0) {
        return status;
    }

    LOG(LOG_ERR, "rcConnect being called.\n");
    irods_conn = rcConnect( myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone, 1, &errMsg );
    LOG(LOG_ERR, "irods_conn is %i\n", irods_conn != nullptr);

    if (irods_conn == NULL) {
        return -1;
    }

    status = clientLogin(irods_conn);
    if (status != 0) {
        rcDisconnect(irods_conn);
        irods_conn = nullptr;
        return -1;
    }

    return 0;
}

