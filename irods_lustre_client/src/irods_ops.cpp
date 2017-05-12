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
#include "../../irods_lustre_api/src/inout_structs.h"
#include "irods_ops.h"

// other includes
#include <string>
#include <stdio.h>
#include <boost/filesystem.hpp>

// TODO change for multithreaded
rcComm_t *irods_conn;

int send_change_map_to_irods(irodsLustreApiInp_t *inp) {

    printf("calling send_change_map_to_irods\n");

    if (!irods_conn) {
        printf("Error:  Called send_change_map_to_irods() without an active irods_conn\n");
        return -1;
    }

    irods::pack_entry_table& pk_tbl = irods::get_pack_table();
    irods::api_entry_table& api_tbl = irods::get_client_api_table();
    init_api_table( api_tbl, pk_tbl );

    void *tmp_out = NULL;
    printf("Before procApiRequest\n");
    int status = procApiRequest( irods_conn, 15001, inp, NULL,
                             &tmp_out, NULL );
    printf("After procApiRequest.  status=%i\n", status);


    if ( status < 0 ) {
        printf( "\n\nERROR - failed to call our api\n\n\n" );
        return status;
    } else {
        irodsLustreApiOut_t* out = static_cast<irodsLustreApiOut_t*>( tmp_out );
        printf("status is %i\n", out->status);
        return out->status;
    }
}

/* The routines below are not longer being used by the iRODS/Lustre connector.
 * These were used when the connector made individual iRODS client calls for each
 * change.  This has been replaced by the API which takes a list of changes and acts
 * upon them.
 *
 * These will be deleted soon.
 */

int update_data_object_metadata(const char *irods_path_cstr, const char *value, const char *meta_type) {

    if (!irods_conn) {
        printf("Error:  Called update_data_object_metadata() without an active irods_conn\n");
        return -1;
    }

    modDataObjMeta_t modDataObjMetaInp;
    keyValPair_t regParam;
    dataObjInfo_t dataObjInfo;

    memset(&regParam, 0, sizeof( regParam ) );
    addKeyVal(&regParam, meta_type, value);

    memset(&dataObjInfo, 0, sizeof(dataObjInfo));
    rstrcpy(dataObjInfo.objPath, irods_path_cstr, MAX_NAME_LEN);

    modDataObjMetaInp.regParam = &regParam;
    modDataObjMetaInp.dataObjInfo = &dataObjInfo;

    int status = rcModDataObjMeta(irods_conn, &modDataObjMetaInp);

    return status;
}

int check_if_data_object_exists(const char *irods_path_cstr, bool &object_exists) {

    if (!irods_conn) {
        printf("Error:  Called object_exists() without an active irods_conn\n");
        return -1;
    }

    boost::filesystem::path path(irods_path_cstr);
    std::string data_object = path.filename().string();
    std::string collection = path.parent_path().string(); 

    genQueryInp_t  gen_inp;
    genQueryOut_t* gen_out = NULL;
    memset(&gen_inp, 0, sizeof(gen_inp));

    std::string query_str = "select DATA_NAME where DATA_NAME = '" + data_object + "' and COLL_NAME = '" +
                     collection + "'";

    fillGenQueryInpFromStrCond((char*)query_str.c_str(), &gen_inp);
    gen_inp.maxRows = MAX_SQL_ROWS;

    int status = rcGenQuery(irods_conn, &gen_inp, &gen_out);

    if ( status < 0 || !gen_out ) {
        freeGenQueryOut(&gen_out);
        clearGenQueryInp(&gen_inp);
        return -1;
    }

    object_exists = gen_out->rowCnt > 0;

    freeGenQueryOut(&gen_out);
    clearGenQueryInp(&gen_inp);

    return 0;
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

int add_avu(const char *irods_path_cstr, const char *attr_cstr, const char *val_cstr, const char *unit_cstr, bool is_collection) {

    if (!irods_conn) {
        printf("Error:  Called add_avu() without an active irods_conn\n");
        return -1;
    }

    modAVUMetadataInp_t mod_inp;
    memset( &mod_inp, 0, sizeof( mod_inp ) );
    const char *op = "add";
    std::string type;

    if (is_collection)
        type = "-C";
    else
        type = "-d";
 
    mod_inp.arg0 = (char*)op;
    mod_inp.arg1 = (char*)type.c_str();
    mod_inp.arg2 = (char*)irods_path_cstr;
    mod_inp.arg3 = (char*)attr_cstr; 
    mod_inp.arg4 = (char*)val_cstr;
    mod_inp.arg5 = (char*)unit_cstr; 

    int status = rcModAVUMetadata(irods_conn, &mod_inp);

    if( status < 0 && CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME != status ) {
        printf("failed to add avu [%s %s %s] on %s\n", attr_cstr, val_cstr, unit_cstr == nullptr ? "NULL" : unit_cstr, irods_path_cstr);
        return -1;
    }

    return 0;
}


int make_collection(const char *irods_path_cstr) {

    if (!irods_conn) {
        printf("Error:  Called make_collection() without an active irods_conn\n");
        return -1;
    }

    int status;
    collInp_t collCreateInp;

    memset(&collCreateInp, 0, sizeof(collCreateInp));
    snprintf(collCreateInp.collName, MAX_NAME_LEN, "%s", irods_path_cstr);

    status = rcCollCreate(irods_conn, &collCreateInp);

    if ( status < 0 && status != CAT_NO_ROWS_FOUND ) {
        printf("rcCollCreate error %i\n", status);
        return status;
    }
    
    return 0; 
}

// irods_path_cstr should be a buffer of size MAX_NAME_LEN
int register_file(const char *src_path_lustre_cstr, const char *irods_path_cstr, const char *resource_name) {

    if (!irods_conn) {
        printf("Error:  Called register_file() without an active irods_conn\n");
        return -1;
    }

    int status;
    dataObjInp_t dataObjOprInp;

    memset(&dataObjOprInp, 0, sizeof(dataObjInp_t));
    addKeyVal(&dataObjOprInp.condInput, DATA_TYPE_KW, "generic");
    addKeyVal(&dataObjOprInp.condInput, FORCE_FLAG_KW, "" );
    addKeyVal(&dataObjOprInp.condInput, DEST_RESC_NAME_KW, resource_name);
    addKeyVal(&dataObjOprInp.condInput, FILE_PATH_KW, src_path_lustre_cstr);

    snprintf(dataObjOprInp.objPath, MAX_NAME_LEN, "%s", irods_path_cstr);

    status = rcPhyPathReg( irods_conn, &dataObjOprInp );
    if ( status < 0 && status != CAT_NO_ROWS_FOUND ) {
        printf("rcPhyPathReg error %i\n", status);
        return status;
    }

    return 0;
}

int find_irods_path_with_avu(const char *attr, const char *value, const char *unit, bool is_collection, char *irods_path) {

    if (!irods_conn) {
        printf("Error:  Called find_irods_path_with_avu() without an active irods_conn\n");
        return -1;
    }

    irods_path[0] = '\0';

    genQueryInp_t  gen_inp;
    genQueryOut_t* gen_out = NULL;
    memset(&gen_inp, 0, sizeof(gen_inp));

    std::string query_str;
    if (is_collection) {
        query_str = "select COLL_NAME where META_COLL_ATTR_NAME = '" + std::string(attr) + "' and META_COLL_ATTR_VALUE = '" + 
                     std::string(value) + "'";
        if (unit != nullptr) {
            query_str += " and META_COLL_ATTR_UNITS = '" + std::string(unit) + "'";
        }
    } else {
        query_str = "select DATA_NAME, COLL_NAME where META_DATA_ATTR_NAME = '" + std::string(attr) + "' and META_DATA_ATTR_VALUE = '" + 
                     std::string(value) + "'";
        if (unit != nullptr) {
            query_str += " and META_DATA_ATTR_UNITS = '" + std::string(unit) + "'";
        }
    }

    fillGenQueryInpFromStrCond((char*)query_str.c_str(), &gen_inp);
    gen_inp.maxRows = MAX_SQL_ROWS;

    int status = rcGenQuery(irods_conn, &gen_inp, &gen_out);

    if ( status < 0 || !gen_out ) {
        freeGenQueryOut(&gen_out);
        clearGenQueryInp(&gen_inp);
        return -1;
    }
 
    if (gen_out->rowCnt < 1) {
        freeGenQueryOut(&gen_out);
        clearGenQueryInp(&gen_inp);
        printf("No object with AVU [%s, %s, %s] found.\n", attr, value, unit == nullptr ? NULL : unit);
        return -1;
    }

    sqlResult_t* coll_names = getSqlResultByInx(gen_out, COL_COLL_NAME);
    const std::string coll_name(&coll_names->value[0]);

    if (!is_collection) {
        sqlResult_t* data_names = getSqlResultByInx(gen_out, COL_DATA_NAME);
        const std::string data_name(&data_names->value[0]);
        std::string path = coll_name + "/" + data_name;
        snprintf(irods_path, MAX_NAME_LEN, "%s", path.c_str());
    } else {
        snprintf(irods_path, MAX_NAME_LEN, "%s", coll_name.c_str());
    } 

    freeGenQueryOut(&gen_out);

    return 0;
}

int remove_data_object(const char *src_path_irods_cstr) {

    if (!irods_conn) {
        printf("Error:  Called remove_data_object() without an active irods_conn\n");
        return -1;
    }

    dataObjInp_t dataObjInp;
    memset(&dataObjInp, 0, sizeof(dataObjInp_t));

    dataObjInp.openFlags = O_RDONLY;

    rstrcpy(dataObjInp.objPath, src_path_irods_cstr, MAX_NAME_LEN);
    int status = rcDataObjUnlink(irods_conn, &dataObjInp);
 
    return status; 
}

int remove_collection(const char *src_path_irods_cstr) {

    if (!irods_conn) {
        printf("Error:  Called remove_collection() without an active irods_conn\n");
        return -1;
    }

    collInp_t collInp;
    memset(&collInp, 0, sizeof(collInp_t));

    rstrcpy(collInp.collName, src_path_irods_cstr, MAX_NAME_LEN);
    int status = rcRmColl(irods_conn, &collInp, false);
    
    return status;
}

int rename_irods_object(const char *src_path_irods_cstr, const char *dest_path_irods_cstr, bool is_collection) {


    if (!irods_conn) {
        printf("Error:  Called rename_data_object() without an active irods_conn\n");
        return -1;
    }

    int rc = 0;

    // if this is a data object, check to see if an object exists at the target location
    // if so delete it.
    if (!is_collection) {
        bool target_data_object_exists;
        check_if_data_object_exists(dest_path_irods_cstr, target_data_object_exists);
        if (target_data_object_exists) {
            rc = remove_data_object(dest_path_irods_cstr);
            if (rc != 0) return rc;
        }
    }


    dataObjCopyInp_t dataObjRenameInp;
    memset(&dataObjRenameInp, 0, sizeof(dataObjCopyInp_t));

    if (is_collection) {
        dataObjRenameInp.srcDataObjInp.oprType = RENAME_COLL;
        dataObjRenameInp.destDataObjInp.oprType = RENAME_COLL;
    } else {
        dataObjRenameInp.srcDataObjInp.oprType = RENAME_DATA_OBJ;
        dataObjRenameInp.destDataObjInp.oprType = RENAME_DATA_OBJ;
    }

    rstrcpy(dataObjRenameInp.destDataObjInp.objPath, dest_path_irods_cstr, MAX_NAME_LEN);
    rstrcpy(dataObjRenameInp.srcDataObjInp.objPath, src_path_irods_cstr, MAX_NAME_LEN);

    int status = rcDataObjRename(irods_conn, &dataObjRenameInp);

    return status;
}

int rename_data_object(const char *src_path_irods_cstr, const char *dest_path_irods_cstr) {
    return rename_irods_object(src_path_irods_cstr, dest_path_irods_cstr, false);
}

int rename_collection(const char *src_path_irods_cstr, const char *dest_path_irods_cstr) {
    return rename_irods_object(src_path_irods_cstr, dest_path_irods_cstr, true);
}
 

int update_vault_path_for_data_object(const char *irods_path_cstr, const char *new_vault_path_cstr) {

    if (!irods_conn) {
        printf("Error:  Called update_vault_path_for_object() without an active irods_conn\n");
        return -1;
    }

    modDataObjMeta_t modDataObjMetaInp;
    keyValPair_t regParam;
    dataObjInfo_t dataObjInfo;

    memset(&regParam, 0, sizeof( regParam ) );
    addKeyVal(&regParam, FILE_PATH_KW, new_vault_path_cstr);

    memset(&dataObjInfo, 0, sizeof(dataObjInfo));
    rstrcpy(dataObjInfo.objPath, irods_path_cstr, MAX_NAME_LEN);

    modDataObjMetaInp.regParam = &regParam;
    modDataObjMetaInp.dataObjInfo = &dataObjInfo;

    int status = rcModDataObjMeta(irods_conn, &modDataObjMetaInp);

    return status;

}

int update_data_object_size(const char *irods_path_cstr, rodsLong_t size) {
    char size_str[MAX_NAME_LEN];
    snprintf(size_str, MAX_NAME_LEN, "%lld", size);
    return update_data_object_metadata(irods_path_cstr, size_str, DATA_SIZE_KW);
}


int update_data_object_modify_time(const char *irods_path_cstr, time_t modify_time) {
    char time_str[MAX_NAME_LEN];
    snprintf(time_str, MAX_NAME_LEN, "%ld", modify_time);
    return update_data_object_metadata(irods_path_cstr, time_str, DATA_MODIFY_KW);
}

}

