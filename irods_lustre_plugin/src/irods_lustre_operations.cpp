// =-=-=-=-=-=-=-
// irods includes
#include "apiHandler.hpp"
#include "irods_stacktrace.hpp"
#include "irods_server_api_call.hpp"
#include "irods_re_serialization.hpp"
#include "objStat.h"
#include "icatHighLevelRoutines.hpp"
#include "irods_virtual_path.hpp"
#include "miscServerFunct.hpp"
#include "irods_configuration_keywords.hpp"
#include "rsRegDataObj.hpp"
#include "rsModAVUMetadata.hpp"
#include "rsCollCreate.hpp"
#include "rsGenQuery.hpp"
#include "rsDataObjUnlink.hpp"
#include "rsRmColl.hpp"
#include "rsDataObjRename.hpp"
#include "rsModDataObjMeta.hpp"

// =-=-=-=-=-=-=-
// // boost includes
#include "boost/lexical_cast.hpp"
#include "boost/filesystem.hpp"

#include "database_routines.hpp"

// =-=-=-=-=-=-=-
// stl includes
#include <sstream>
#include <string>
#include <iostream>
#include <vector>

// capn proto
#pragma push_macro("LIST")
#undef LIST

#pragma push_macro("ERROR")
#undef ERROR

#include "../../lustre_irods_connector/src/change_table.capnp.h"
#include <capnp/message.h>
#include <capnp/serialize-packed.h>

#pragma pop_macro("LIST")
#pragma pop_macro("ERROR")

#include "inout_structs.h"
#include "database_routines.hpp"
#include "irods_lustre_operations.hpp"

#define MAX_BIND_VARS 32000
extern const char *cllBindVars[MAX_BIND_VARS];
extern int cllBindVarCount;

const std::string fidstr_avu_key = "lustre_identifier";

const std::string update_data_size_sql = "update R_DATA_MAIN set data_size = ? where data_id = (select * from ("
                   "select R_DATA_MAIN.data_id "
                   "from R_DATA_MAIN "
                   "inner join R_OBJT_METAMAP on R_DATA_MAIN.data_id = R_OBJT_METAMAP.object_id "
                   "inner join R_META_MAIN on R_META_MAIN.meta_id = R_OBJT_METAMAP.meta_id "
                   "where R_META_MAIN.meta_attr_name = '" + fidstr_avu_key + "' and R_META_MAIN.meta_attr_value = ?)temp_table)";

const std::string update_data_object_for_rename_sql = "update R_DATA_MAIN set data_name = ?, data_path = ?, coll_id = (select * from ("
                   "select R_COLL_MAIN.coll_id "
                   "from R_COLL_MAIN "
                   "inner join R_OBJT_METAMAP on R_COLL_MAIN.coll_id = R_OBJT_METAMAP.object_id "
                   "inner join R_META_MAIN on R_META_MAIN.meta_id = R_OBJT_METAMAP.meta_id "
                   "where R_META_MAIN.meta_attr_name = '" + fidstr_avu_key + "' and R_META_MAIN.meta_attr_value = ?)temp_table)"
                   "where data_id = (select * from ("
                   "select R_DATA_MAIN.data_id "
                   "from R_DATA_MAIN "
                   "inner join R_OBJT_METAMAP on R_DATA_MAIN.data_id = R_OBJT_METAMAP.object_id "
                   "inner join R_META_MAIN on R_META_MAIN.meta_id = R_OBJT_METAMAP.meta_id "
                   "where R_META_MAIN.meta_attr_name = '" + fidstr_avu_key + "' and R_META_MAIN.meta_attr_value = ?)temp_table2)";

const std::string get_collection_path_from_fidstr_sql = "select R_COLL_MAIN.coll_name "
                   "from R_COLL_MAIN "
                   "inner join R_OBJT_METAMAP on R_COLL_MAIN.coll_id = R_OBJT_METAMAP.object_id "
                   "inner join R_META_MAIN on R_META_MAIN.meta_id = R_OBJT_METAMAP.meta_id "
                   "where R_META_MAIN.meta_attr_name = '" + fidstr_avu_key + "' and R_META_MAIN.meta_attr_value = ?";


const std::string update_collection_for_rename_sql = "update R_COLL_MAIN set coll_name = ?, parent_coll_name = ? "
                   "where coll_id = (select * from ("
                   "select R_COLL_MAIN.coll_id "
                   "from R_COLL_MAIN "
                   "inner join R_OBJT_METAMAP on R_COLL_MAIN.coll_id = R_OBJT_METAMAP.object_id "
                   "inner join R_META_MAIN on R_META_MAIN.meta_id = R_OBJT_METAMAP.meta_id "
                   "where R_META_MAIN.meta_attr_name = '" + fidstr_avu_key + "' and R_META_MAIN.meta_attr_value = ?)temp_table)";

const std::string remove_object_meta_sql = "delete from R_OBJT_METAMAP where object_id = (select * from ("
                   "select R_OBJT_METAMAP.object_id "
                   "from R_OBJT_METAMAP "
                   "inner join R_META_MAIN on R_META_MAIN.meta_id = R_OBJT_METAMAP.meta_id "
                   "where R_META_MAIN.meta_attr_name = '" + fidstr_avu_key + "' and R_META_MAIN.meta_attr_value = ?)temp_table)";

const std::string unlink_sql = "delete from R_DATA_MAIN where data_id = (select * from ("
                   "select R_DATA_MAIN.data_id "
                   "from R_DATA_MAIN "
                   "inner join R_OBJT_METAMAP on R_DATA_MAIN.data_id = R_OBJT_METAMAP.object_id "
                   "inner join R_META_MAIN on R_META_MAIN.meta_id = R_OBJT_METAMAP.meta_id "
                   "where R_META_MAIN.meta_attr_name = '" + fidstr_avu_key + "' and R_META_MAIN.meta_attr_value = ?)temp_table)";

const std::string rmdir_sql = "delete from R_COLL_MAIN where coll_id = (select * from ("
                   "select R_COLL_MAIN.coll_id "
                   "from R_COLL_MAIN "
                   "inner join R_OBJT_METAMAP on R_COLL_MAIN.coll_id = R_OBJT_METAMAP.object_id "
                   "inner join R_META_MAIN on R_META_MAIN.meta_id = R_OBJT_METAMAP.meta_id "
                   "where R_META_MAIN.meta_attr_name = '" + fidstr_avu_key + "' and R_META_MAIN.meta_attr_value = ?)temp_table)";

const std::string get_collection_id_from_fidstr_sql = "select R_COLL_MAIN.coll_id "
                   "from R_COLL_MAIN "
                   "inner join R_OBJT_METAMAP on R_COLL_MAIN.coll_id = R_OBJT_METAMAP.object_id "
                   "inner join R_META_MAIN on R_META_MAIN.meta_id = R_OBJT_METAMAP.meta_id "
                   "where R_META_MAIN.meta_attr_name = '" + fidstr_avu_key + "' and R_META_MAIN.meta_attr_value = ?";


const std::string insert_data_obj_sql = "insert into R_DATA_MAIN (data_id, coll_id, data_name, data_repl_num, data_type_name, "
                   "data_size, resc_name, data_path, data_owner_name, data_owner_zone, data_is_dirty, data_map_id, resc_id) "
                   "values (?, ?, ?, 0, 'generic', ?, 'EMPTY_RESC_NAME', ?, ?, ?, 0, 0, ?)";

const std::string insert_user_ownership_data_object_sql = "insert into R_OBJT_ACCESS (object_id, user_id, access_type_id) values (?, ?, 1200)";

const std::string get_user_id_sql = "select user_id from R_USER_MAIN where user_name = ?";

#ifdef POSTGRES_ICAT

const std::string update_filepath_on_collection_rename_sql = "update R_DATA_MAIN set data_path = overlay(data_path placing ? from 1 for char_length(?)) where data_path like ?";

#endif

// finds an irods object with the attr/val/unit combination, returns the first entry in the list
int find_irods_path_with_avu(rsComm_t *_conn, const std::string& attr, const std::string& value, const std::string& unit, bool is_collection, std::string& irods_path) {

    genQueryInp_t  gen_inp;
    genQueryOut_t* gen_out = NULL;
    memset(&gen_inp, 0, sizeof(gen_inp));

    std::string query_str;
    if (is_collection) {
        query_str = "select COLL_NAME where META_COLL_ATTR_NAME = '" + attr + "' and META_COLL_ATTR_VALUE = '" +
                     value + "'";
        if (unit != "") {
            query_str += " and META_COLL_ATTR_UNITS = '" + unit + "'";
        }
    } else {
        query_str = "select DATA_NAME, COLL_NAME where META_DATA_ATTR_NAME = '" + attr + "' and META_DATA_ATTR_VALUE = '" +
                     value + "'";
        if (unit != "") {
            query_str += " and META_DATA_ATTR_UNITS = '" + unit + "'";
        }
    }

    fillGenQueryInpFromStrCond((char*)query_str.c_str(), &gen_inp);
    gen_inp.maxRows = MAX_SQL_ROWS;

    int status = rsGenQuery(_conn, &gen_inp, &gen_out);

    if ( status < 0 || !gen_out ) {
        freeGenQueryOut(&gen_out);
        clearGenQueryInp(&gen_inp);
        return -1;
    }

    if (gen_out->rowCnt < 1) {
        freeGenQueryOut(&gen_out);
        clearGenQueryInp(&gen_inp);
        rodsLog(LOG_NOTICE, "No object with AVU [%s, %s, %s] found.\n", attr.c_str(), value.c_str(), unit == "" ? "null": unit.c_str());
        return -1;
    }

    sqlResult_t* coll_names = getSqlResultByInx(gen_out, COL_COLL_NAME);
    const std::string coll_name(&coll_names->value[0]);

    if (!is_collection) {
        sqlResult_t* data_names = getSqlResultByInx(gen_out, COL_DATA_NAME);
        const std::string data_name(&data_names->value[0]);
        irods_path = coll_name + "/" + data_name;
    } else {
        irods_path = coll_name;
    }

    freeGenQueryOut(&gen_out);

    return 0;
}




// Returns the path in irods for a file in lustre.  
// Precondition:  irods_path is a buffer of size MAX_NAME_LEN
int lustre_path_to_irods_path(const char *lustre_path, const char *lustre_root_path, 
        const char *register_path, char *irods_path) {

    // make sure the file is underneath the lustre_root_path
    if (strncmp(lustre_path, lustre_root_path, strlen(lustre_root_path)) != 0) {
        return -1;
    }

    snprintf(irods_path, MAX_NAME_LEN, "%s%s", register_path, lustre_path + strlen(lustre_root_path));

    return 0;
}

int get_user_id(rsComm_t* _comm, icatSessionStruct *icss, rodsLong_t& user_id, bool direct_db_access_flag) {

    std::vector<std::string> bindVars;
    bindVars.push_back(_comm->clientUser.userName);
    int status = cmlGetIntegerValueFromSql(get_user_id_sql.c_str(), &user_id, bindVars, icss );
    if (status != 0) {
       return SYS_USER_RETRIEVE_ERR;
    }
    return 0;
}
 
void handle_create(const std::string& lustre_root_path, const std::string& register_path, 
        const int64_t& resource_id, const std::string& resource_name, const std::string& fidstr, 
        const std::string& lustre_path, const std::string& object_name, 
        const ChangeDescriptor::ObjectTypeEnum& object_type, const std::string& parent_fidstr, const int64_t& file_size,
        rsComm_t* _comm, icatSessionStruct *icss, const rodsLong_t& user_id, bool direct_db_access_flag) {


    int status;
   
    char irods_path[MAX_NAME_LEN];
    if (lustre_path_to_irods_path(lustre_path.c_str(), lustre_root_path.c_str(), register_path.c_str(), irods_path) < 0) {
        rodsLog(LOG_NOTICE, "Skipping entry because lustre_path [%s] is not within lustre_root_path [%s].",
                   lustre_path.c_str(), lustre_root_path.c_str()); 
        return;
    }


    if (direct_db_access_flag) { 

        // register object
        
        int seq_no = cmlGetCurrentSeqVal(icss);
        std::string username = _comm->clientUser.userName;
        std::string zone = _comm->clientUser.rodsZone;
        rodsLog(LOG_NOTICE, "seq_no=%i username=%s zone=%s", seq_no, username.c_str(), zone.c_str());
        rodsLog(LOG_NOTICE, "object_name = %s", object_name.c_str());

        //boost::filesystem::path p(irods_path);

        // get collection id from parent fidstr
        rodsLong_t coll_id;
        std::vector<std::string> bindVars;
        bindVars.push_back(parent_fidstr);
        status = cmlGetIntegerValueFromSql(get_collection_id_from_fidstr_sql.c_str(), &coll_id, bindVars, icss );
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error during registration object %s.  Error getting collection id for collection with fidstr=%s.  Error is %i", 
                    fidstr.c_str(), parent_fidstr.c_str(),  status);
            return;
        }

        // insert data object
        cllBindVars[0] = std::to_string(seq_no).c_str();
        cllBindVars[1] = std::to_string(coll_id).c_str();
        cllBindVars[2] = object_name.c_str();
        cllBindVars[3] = std::to_string(file_size).c_str();  
        cllBindVars[4] = lustre_path.c_str(); 
        cllBindVars[5] = _comm->clientUser.userName;
        cllBindVars[6] = _comm->clientUser.rodsZone;
        cllBindVars[7] = std::to_string(resource_id).c_str(); 
        cllBindVarCount = 8;
        status = cmlExecuteNoAnswerSql(insert_data_obj_sql.c_str(), icss);
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error registering object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }

        status =  cmlExecuteNoAnswerSql("commit", icss);
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error committing insertion of new data_object %s.  Error is %i", fidstr.c_str(), status);
            return;
        } 

        // insert user ownership
        cllBindVars[0] = std::to_string(seq_no).c_str();
        cllBindVars[1] = std::to_string(user_id).c_str();
        cllBindVarCount = 2;
        status = cmlExecuteNoAnswerSql(insert_user_ownership_data_object_sql.c_str(), icss);
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error adding onwership to object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }

        status =  cmlExecuteNoAnswerSql("commit", icss);
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error committing ownership of new data_object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }

        // add lustre_identifier metadata
        keyValPair_t reg_param;
        memset(&reg_param, 0, sizeof(reg_param));
        addKeyVal(&reg_param, fidstr_avu_key.c_str(), fidstr.c_str());
        status = chlAddAVUMetadata(_comm, 0, "-d", irods_path, fidstr_avu_key.c_str(), fidstr.c_str(), "");
        rodsLog(LOG_NOTICE, "Return value from chlAddAVUMetdata = %i", status);
        if (status < 0) {
            rodsLog(LOG_ERROR, "Error adding %s metadata to object %s.  Error is %i", fidstr_avu_key.c_str(), fidstr.c_str(), status);
            return;
        }

    } else {

        // register object
        dataObjInfo_t dataObjInfo;
        dataObjInfo_t *outDataObjInfo;
        memset( &dataObjInfo, 0, sizeof( dataObjInfo) );
        strncpy(dataObjInfo.objPath, irods_path, MAX_NAME_LEN);
        strncpy(dataObjInfo.filePath, lustre_path.c_str(), MAX_NAME_LEN);
        strncpy(dataObjInfo.rescName, "EMPTY_RESOURCE_NAME", NAME_LEN);
        strncpy(dataObjInfo.rescHier, resource_name.c_str(), NAME_LEN);
        dataObjInfo.rescId = resource_id;
        dataObjInfo.dataSize = file_size; 
        strncpy(dataObjInfo.dataType, "generic", NAME_LEN);
        strncpy(dataObjInfo.version, "0", NAME_LEN);
        status = rsRegDataObj(_comm, &dataObjInfo, &outDataObjInfo); 
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error registering object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }
        freeDataObjInfo(outDataObjInfo);

        // add lustre_identifier metadata
        modAVUMetadataInp_t modAVUMetadataInp;
        memset(&modAVUMetadataInp, 0, sizeof(modAVUMetadataInp_t)); 
        modAVUMetadataInp.arg0 = "add";
        modAVUMetadataInp.arg1 = "-d";
        modAVUMetadataInp.arg2 = irods_path;
        modAVUMetadataInp.arg3 = const_cast<char*>(fidstr_avu_key.c_str());
        modAVUMetadataInp.arg4 = const_cast<char*>(fidstr.c_str());
        status = rsModAVUMetadata(_comm, &modAVUMetadataInp);
        if (status < 0) {
            rodsLog(LOG_ERROR, "Error adding %s metadata to object %s.  Error is %i", fidstr_avu_key.c_str(), fidstr.c_str(), status);
            return;
        }


    }
}

void handle_mkdir(const std::string& lustre_root_path, const std::string& register_path, 
        const int64_t& resource_id, const std::string& resource_name, const std::string& fidstr, 
        const std::string& lustre_path, const std::string& object_name, 
        const ChangeDescriptor::ObjectTypeEnum& object_type, const std::string& parent_fidstr, const int64_t& file_size,
        rsComm_t* _comm, icatSessionStruct *icss, const rodsLong_t& user_id, bool direct_db_access_flag) {


    int status;

    char irods_path[MAX_NAME_LEN];
    if (lustre_path_to_irods_path(lustre_path.c_str(), lustre_root_path.c_str(), register_path.c_str(), irods_path) < 0) {
        rodsLog(LOG_NOTICE, "Skipping mkdir on lustre_path [%s] which is not within lustre_root_path [%s].",
               lustre_path.c_str(), lustre_root_path.c_str());
        return;
    }

    if (direct_db_access_flag) { 

        collInfo_t coll_info;
        memset(&coll_info, 0, sizeof(coll_info));
        strncpy(coll_info.collName, irods_path, MAX_NAME_LEN);

        // register object
        status = chlRegColl(_comm, &coll_info);
        rodsLog(LOG_NOTICE, "Return value from chlRegColl = %i", status);
        if (status < 0) {
            rodsLog(LOG_ERROR, "Error registering collection %s.  Error is %i", fidstr.c_str(), status);
            return;
        } 

        // add lustre_identifier metadata
        keyValPair_t reg_param;
        memset(&reg_param, 0, sizeof(reg_param));
        addKeyVal(&reg_param, fidstr_avu_key.c_str(), fidstr.c_str());
        status = chlAddAVUMetadata(_comm, 0, "-C", irods_path, fidstr_avu_key.c_str(), fidstr.c_str(), "");
        rodsLog(LOG_NOTICE, "Return value from chlAddAVUMetadata = %i", status);
        if (status < 0) {
            rodsLog(LOG_ERROR, "Error adding %s metadata to object %s.  Error is %i", fidstr_avu_key.c_str(), fidstr.c_str(), status);
            return;
        }

    } else {


        // register object
        collInp_t coll_input;
        memset(&coll_input, 0, sizeof(coll_input));
        strncpy(coll_input.collName, irods_path, MAX_NAME_LEN);
        status = rsCollCreate(_comm, &coll_input); 
        if (status < 0) {
            rodsLog(LOG_ERROR, "Error registering collection %s.  Error is %i", fidstr.c_str(), status);
            return;
        } 

        // add lustre_identifier metadata
        modAVUMetadataInp_t modAVUMetadataInp;
        memset(&modAVUMetadataInp, 0, sizeof(modAVUMetadataInp_t)); 
        modAVUMetadataInp.arg0 = "add";
        modAVUMetadataInp.arg1 = "-C";
        modAVUMetadataInp.arg2 = irods_path;
        modAVUMetadataInp.arg3 = const_cast<char*>(fidstr_avu_key.c_str());
        modAVUMetadataInp.arg4 = const_cast<char*>(fidstr.c_str());
        status = rsModAVUMetadata(_comm, &modAVUMetadataInp);
        if (status < 0) {
            rodsLog(LOG_ERROR, "Error adding %s metadata to object %s.  Error is %i", fidstr_avu_key.c_str(), fidstr.c_str(), status);
            return;
        }


    }

}

void handle_other(const std::string& lustre_root_path, const std::string& register_path, 
        const int64_t& resource_id, const std::string& resource_name, const std::string& fidstr, 
        const std::string& lustre_path, const std::string& object_name, 
        const ChangeDescriptor::ObjectTypeEnum& object_type, const std::string& parent_fidstr, const int64_t& file_size,
        rsComm_t* _comm, icatSessionStruct *icss, const rodsLong_t& user_id, bool direct_db_access_flag) {

    int status;

    if (direct_db_access_flag) { 

        // read and update the file size
        cllBindVars[0] = std::to_string(file_size).c_str(); //file_size_str.c_str();
        cllBindVars[1] = fidstr.c_str(); 
        cllBindVarCount = 2;
        status = cmlExecuteNoAnswerSql(update_data_size_sql.c_str(), icss);

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error updating data_object_size for data_object %s.  Error is %i", fidstr.c_str(), status);
            cmlExecuteNoAnswerSql("rollback", icss);
            return;
        }

        status =  cmlExecuteNoAnswerSql("commit", icss);

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error committing update to data_object_size for data_object %s.  Error is %i", fidstr.c_str(), status);
            return;
        } 

    } else {

        std::string irods_path;
       
        // look up object based on fidstr
        status = find_irods_path_with_avu(_comm, fidstr_avu_key, fidstr, "", false, irods_path); 

        // modify the file size
        modDataObjMeta_t modDataObjMetaInp;
        dataObjInfo_t dataObjInfo;
        memset( &modDataObjMetaInp, 0, sizeof( modDataObjMetaInp ) );
        memset( &dataObjInfo, 0, sizeof( dataObjInfo ) );


        modDataObjMetaInp.dataObjInfo = &dataObjInfo;
        dataObjInfo.dataSize = file_size; 
        strncpy(dataObjInfo.filePath, lustre_path.c_str(), MAX_NAME_LEN);

        status = rsModDataObjMeta( _comm, &modDataObjMetaInp );

        if ( status < 0 ) {
            rodsLog(LOG_ERROR, "Error updating data object rename for data_object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }

    }

}

void handle_rename_file(const std::string& lustre_root_path, const std::string& register_path, 
        const int64_t& resource_id, const std::string& resource_name, const std::string& fidstr, 
        const std::string& lustre_path, const std::string& object_name, 
        const ChangeDescriptor::ObjectTypeEnum& object_type, const std::string& parent_fidstr, const int64_t& file_size,
        rsComm_t* _comm, icatSessionStruct *icss, const rodsLong_t& user_id, bool direct_db_access_flag) {

    int status;

    if (direct_db_access_flag) { 

        // update data_name, data_path, and coll_id
        cllBindVars[0] = object_name.c_str();
        cllBindVars[1] = lustre_path.c_str();
        cllBindVars[2] = parent_fidstr.c_str();
        cllBindVars[3] = fidstr.c_str();
        cllBindVarCount = 4;
        status = cmlExecuteNoAnswerSql(update_data_object_for_rename_sql.c_str(), icss);

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error updating data object rename for data_object %s.  Error is %i", fidstr.c_str(), status);
            cmlExecuteNoAnswerSql("rollback", icss);
            return;
        }

        status =  cmlExecuteNoAnswerSql("commit", icss);

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error committing update to data object rename for data_object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }
    } else {

        std::string old_irods_path;
        std::string new_parent_irods_path;

        // look up object based on fidstr
        status = find_irods_path_with_avu(_comm, fidstr_avu_key, fidstr, "", false, old_irods_path); 
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error renaming data object %s.  Could not find object by fidstr.", fidstr.c_str());
            return;
        }

        // look up new parent path based on parent fidstr
        status = find_irods_path_with_avu(_comm, fidstr_avu_key, parent_fidstr, "", true, new_parent_irods_path); 
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error renaming data object %s.  Could not find object by fidstr.", parent_fidstr.c_str());
            return;
        }

        std::string new_irods_path = new_parent_irods_path + "/" + object_name;

        // rename the data object 
        dataObjCopyInp_t dataObjRenameInp;
        memset( &dataObjRenameInp, 0, sizeof( dataObjRenameInp ) );

        strncpy(dataObjRenameInp.srcDataObjInp.objPath, old_irods_path.c_str(), MAX_NAME_LEN);
        strncpy(dataObjRenameInp.destDataObjInp.objPath, new_irods_path.c_str(), MAX_NAME_LEN);

        dataObjRenameInp.srcDataObjInp.oprType = dataObjRenameInp.destDataObjInp.oprType = RENAME_DATA_OBJ;

        if ( status < 0 ) {
            rodsLog(LOG_ERROR, "Error updating data object rename for data_object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }

        // modify the file path
        modDataObjMeta_t modDataObjMetaInp;
        dataObjInfo_t dataObjInfo;
        memset( &modDataObjMetaInp, 0, sizeof( modDataObjMetaInp ) );
        memset( &dataObjInfo, 0, sizeof( dataObjInfo ) );

        modDataObjMetaInp.dataObjInfo = &dataObjInfo;
        strncpy(dataObjInfo.objPath, new_irods_path.c_str(), MAX_NAME_LEN);
        strncpy(dataObjInfo.filePath, lustre_path.c_str(), MAX_NAME_LEN);

        status = rsModDataObjMeta( _comm, &modDataObjMetaInp );

        if ( status < 0 ) {
            rodsLog(LOG_ERROR, "Error updating data object rename for data_object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }


    }

}

void handle_rename_dir(const std::string& lustre_root_path, const std::string& register_path, 
        const int64_t& resource_id, const std::string& resource_name, const std::string& fidstr, 
        const std::string& lustre_path, const std::string& object_name, 
        const ChangeDescriptor::ObjectTypeEnum& object_type, const std::string& parent_fidstr, const int64_t& file_size,
        rsComm_t* _comm, icatSessionStruct *icss, const rodsLong_t& user_id, bool direct_db_access_flag) {

    int status;

    // determine the old irods path and new irods path for the collection
    std::string old_irods_path;
    std::string new_parent_irods_path;
    std::string new_irods_path;

    // look up the old irods path for the collection based on fidstr
    status = find_irods_path_with_avu(_comm, fidstr_avu_key, fidstr, "", true, old_irods_path); 
    if (status != 0) {
    rodsLog(LOG_ERROR, "Error renaming data object %s.  Could not find object by fidstr.", fidstr.c_str());
        return;
    }

    // look up new parent path based on the new parent fidstr
    status = find_irods_path_with_avu(_comm, fidstr_avu_key, parent_fidstr, "", true, new_parent_irods_path); 
    if (status != 0) {
        rodsLog(LOG_ERROR, "Error renaming data object %s.  Could not find object by fidstr.", parent_fidstr.c_str());
        return;
    }

    // use object_name to get new irods path
    new_irods_path = new_parent_irods_path + "/" + object_name;

 
    if (direct_db_access_flag) { 

        char parent_path[MAX_NAME_LEN];
        char collection_path[MAX_NAME_LEN];

        // get the parent's path - using parent's fidstr
        std::vector<std::string> bindVars;
        bindVars.push_back(parent_fidstr);
        status = cmlGetStringValueFromSql(get_collection_path_from_fidstr_sql.c_str(), parent_path, MAX_NAME_LEN, bindVars, icss);
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error looking up parent collection for rename for collection %s.  Error is %i", fidstr.c_str(), status);
            cmlExecuteNoAnswerSql("rollback", icss);
        }

        snprintf(collection_path, MAX_NAME_LEN, "%s%s%s", parent_path, irods::get_virtual_path_separator().c_str(), object_name.c_str());

        rodsLog(LOG_NOTICE, "collection path = %s\tparent_path = %s", collection_path, parent_path);
          

        // update coll_name, parent_coll_name, and coll_id
        cllBindVars[0] = collection_path;
        cllBindVars[1] = parent_path;
        cllBindVars[2] = fidstr.c_str();
        cllBindVarCount = 3;
        status = cmlExecuteNoAnswerSql(update_collection_for_rename_sql.c_str(), icss);

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error updating collection object rename for collection %s.  Error is %i", fidstr.c_str(), status);
            cmlExecuteNoAnswerSql("rollback", icss);
            return;
        }

        status =  cmlExecuteNoAnswerSql("commit", icss);

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error committing update to collection rename for collection %s.  Error is %i", fidstr.c_str(), status);
            return;
        }

        // TODO: Issue 6 - Must rename all file paths for all underlying data objects

    } else {

        // rename the data object 
        dataObjCopyInp_t dataObjRenameInp;
        memset( &dataObjRenameInp, 0, sizeof( dataObjRenameInp ) );

        strncpy(dataObjRenameInp.srcDataObjInp.objPath, old_irods_path.c_str(), MAX_NAME_LEN);
        strncpy(dataObjRenameInp.destDataObjInp.objPath, new_irods_path.c_str(), MAX_NAME_LEN);

        dataObjRenameInp.srcDataObjInp.oprType = dataObjRenameInp.destDataObjInp.oprType = RENAME_COLL;

        if ( status < 0 ) {
            rodsLog(LOG_ERROR, "Error updating data object rename for data_object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }

        // TODO: Issue 6 - Must rename all file paths for all underlying data objects

    }

    // determine the old and new lustre paths and update all data objects
    try {

        std::string old_lustre_path = lustre_root_path + old_irods_path.substr(register_path.length());
        std::string new_lustre_path = lustre_root_path + new_irods_path.substr(register_path.length());
        std::string like_clause = old_lustre_path + "/%";

        rodsLog(LOG_NOTICE, "old_lustre_path = %s\tnew_lustre_path = %s", old_lustre_path.c_str(), new_lustre_path.c_str());

        // for now, rename all with sql update
        cllBindVars[0] = new_lustre_path.c_str();
        cllBindVars[1] = old_lustre_path.c_str();
        cllBindVars[2] = like_clause.c_str();
        cllBindVarCount = 3;
        status = cmlExecuteNoAnswerSql(update_filepath_on_collection_rename_sql.c_str(), icss);

        if ( status < 0 && status != CAT_SUCCESS_BUT_WITH_NO_INFO) {
            rodsLog(LOG_ERROR, "Error updating data objects after collection move for collection %s.  Error is %i", fidstr.c_str(), status);
            cmlExecuteNoAnswerSql("rollback", icss);
            return;
        }

        status =  cmlExecuteNoAnswerSql("commit", icss);
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error committing data object update after collection move for collection %s.  Error is %i", fidstr.c_str(), status);
            return;
        } 


    } catch(const std::out_of_range& e) {
        rodsLog(LOG_ERROR, "Error updating data objects after collection move for collection %s.  Error is %i", fidstr.c_str(), status);
        return;
    }

}

void handle_unlink(const std::string& lustre_root_path, const std::string& register_path, 
        const int64_t& resource_id, const std::string& resource_name, const std::string& fidstr, 
        const std::string& lustre_path, const std::string& object_name, 
        const ChangeDescriptor::ObjectTypeEnum& object_type, const std::string& parent_fidstr, const int64_t& file_size,
        rsComm_t* _comm, icatSessionStruct *icss, const rodsLong_t& user_id, bool direct_db_access_flag) {

    int status;

    if (direct_db_access_flag) { 

        cllBindVars[0] = fidstr.c_str();
        cllBindVarCount = 1;
        status = cmlExecuteNoAnswerSql(unlink_sql.c_str(), icss);

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error deleting data object %s.  Error is %i", fidstr.c_str(), status);
            cmlExecuteNoAnswerSql("rollback", icss);
            return;
        }

        // delete the metadata on the data object 
        cllBindVars[0] = fidstr.c_str();
        cllBindVarCount = 1;
        status = cmlExecuteNoAnswerSql(remove_object_meta_sql.c_str(), icss);

        if (status != 0) {
            // Couldn't delete metadata.  Just log and return 
            rodsLog(LOG_ERROR, "Error deleting metadata from data object %s.  Error is %i", fidstr.c_str(), status);
        }


        status =  cmlExecuteNoAnswerSql("commit", icss);

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error committing delete for data object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }

    } else {

        std::string irods_path;
       
        // look up object based on fidstr
        status = find_irods_path_with_avu(_comm, fidstr_avu_key, fidstr, "", false, irods_path); 

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error unregistering data object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }

        // unregister the data object
        dataObjInp_t dataObjInp;
        memset(&dataObjInp, 0, sizeof(dataObjInp));
        strncpy(dataObjInp.objPath, irods_path.c_str(), MAX_NAME_LEN);
        dataObjInp.oprType = UNREG_OPR;
        
        status = rsDataObjUnlink(_comm, &dataObjInp);

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error unregistering data object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }


    }
}

void handle_rmdir(const std::string& lustre_root_path, const std::string& register_path, 
        const int64_t& resource_id, const std::string& resource_name, const std::string& fidstr, 
        const std::string& lustre_path, const std::string& object_name, 
        const ChangeDescriptor::ObjectTypeEnum& object_type, const std::string& parent_fidstr, const int64_t& file_size,
        rsComm_t* _comm, icatSessionStruct *icss, const rodsLong_t& user_id, bool direct_db_access_flag) {

    int status;

    if (direct_db_access_flag) { 


        cllBindVars[0] = fidstr.c_str();
        cllBindVarCount = 1;
        status = cmlExecuteNoAnswerSql(rmdir_sql.c_str(), icss);

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error deleting directory %s.  Error is %i", fidstr.c_str(), status);
            cmlExecuteNoAnswerSql("rollback", icss);
            return;
        }

        // delete the metadata on the collection 
        cllBindVars[0] = fidstr.c_str();
        cllBindVarCount = 1;
        status = cmlExecuteNoAnswerSql(remove_object_meta_sql.c_str(), icss);

        if (status != 0) {
            // Couldn't delete metadata.  Just log and return.
            rodsLog(LOG_ERROR, "Error deleting metadata from collection %s.  Error is %i", fidstr.c_str(), status);
        }

        status =  cmlExecuteNoAnswerSql("commit", icss);

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error committing delete for collection %s.  Error is %i", fidstr.c_str(), status);
            return;
        }

    } else {

        std::string irods_path;
       
        // look up object based on fidstr
        status = find_irods_path_with_avu(_comm, fidstr_avu_key, fidstr, "", true, irods_path); 

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error deleting directory %s.  Error is %i", fidstr.c_str(), status);
            return;
        }

        // remove the collection 
        collInp_t rmCollInp;
        memset(&rmCollInp, 0, sizeof(rmCollInp));
        strncpy(rmCollInp.collName, irods_path.c_str(), MAX_NAME_LEN);
        //rmCollInp.oprType = UNREG_OPR;
        
        status = rsRmColl(_comm, &rmCollInp, nullptr);

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error deleting directory %s.  Error is %i", fidstr.c_str(), status);
            return;
        }


    }
}
