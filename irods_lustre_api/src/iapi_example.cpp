/*
 * iapitest - test pluggable apis
*/
#include "irods_client_api_table.hpp"
#include "irods_pack_table.hpp"
#include "rodsClient.h"
#include "parseCommandLine.h"
#include "rodsPath.h"
#include "lsUtil.h"
#include "irods_buffer_encryption.hpp"
#include <string>
#include <iostream>

#include "inout_structs.h"


void usage();

int main( int, char** ) {

    signal( SIGPIPE, SIG_IGN );


    rodsEnv myEnv;
    int status = getRodsEnv( &myEnv );
    if ( status < 0 ) {
        rodsLogError( LOG_ERROR, status, "main: getRodsEnv error. " );
        exit( 1 );
    }

    rErrMsg_t errMsg;
    rcComm_t *conn;
    conn = rcConnect(
               myEnv.rodsHost,
               myEnv.rodsPort,
               myEnv.rodsUserName,
               myEnv.rodsZone,
               0, &errMsg );

    if ( conn == NULL ) {
        exit( 2 );
    }

    // =-=-=-=-=-=-=-
    // initialize pluggable api table
    irods::pack_entry_table& pk_tbl = irods::get_pack_table();
    irods::api_entry_table& api_tbl = irods::get_client_api_table();
    init_api_table( api_tbl, pk_tbl );

    if ( strcmp( myEnv.rodsUserName, PUBLIC_USER_NAME ) != 0 ) {
        status = clientLogin( conn );
        if ( status != 0 ) {
            rcDisconnect( conn );
            exit( 7 );
        }
    }

    irodsLustreApiInp_t inp;
    memset( &inp, 0, sizeof( inp ) );
    /*inp.change_log_json = "{"
            "\"employee\": {"
            "\"name\": \"justin james\","
            "\"dob\": \"08/27/1971\"}"
            "}";*/

    void *tmp_out = NULL;
    status = procApiRequest( conn, 15001, &inp, NULL,
                             &tmp_out, NULL );
    if ( status < 0 ) {
        printf( "\n\nERROR - failed to call our api\n\n\n" );
        return 0;
    }
    else {
        irodsLustreApiOut_t* out = static_cast<irodsLustreApiOut_t*>( tmp_out );
        printf("status is %i\n", out->status);
    }

    rcDisconnect( conn );
}

