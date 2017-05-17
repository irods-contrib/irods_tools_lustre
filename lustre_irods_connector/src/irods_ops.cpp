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

// local includes
//#include "../../irods_lustre_api/src/inout_structs.h"
#include "inout_structs.h"
#include "irods_ops.h"
#include "logging.hpp"

// other includes
#include <string>
#include <stdio.h>
#include <boost/filesystem.hpp>

// TODO change for multithreaded
rcComm_t *irods_conn;

int send_change_map_to_irods(irodsLustreApiInp_t *inp) {

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

extern "C" {

int instantiate_irods_connection() {

    rodsEnv myEnv;
    int status;
    rErrMsg_t errMsg;

    status = getRodsEnv( &myEnv );
    if (status < 0) {
        return status;
    }

    irods_conn = rcConnect( myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName, myEnv.rodsZone, 1, &errMsg );

    if (irods_conn == NULL) {
        return -1;
    }

    status = clientLogin(irods_conn);
    if (status != 0) {
        rcDisconnect(irods_conn);
        return -1;
    }

    return 0;
}

void disconnect_irods_connection() {
    if (irods_conn)
        rcDisconnect(irods_conn);
    irods_conn = nullptr;
}

}


