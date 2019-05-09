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
#include "rsPhyPathReg.hpp"
#include "rodsType.h"

#if defined(COCKROACHDB_ICAT)
  #include "mid_level_cockroachdb.hpp"
  #include "low_level_cockroachdb.hpp"
#else
  #include "mid_level_other.hpp"
  #include "low_level_odbc_other.hpp"
#endif

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

#if defined(POSTGRES_ICAT) 
    const std::string update_filepath_on_collection_rename_sql = "update R_DATA_MAIN set data_path = overlay(data_path placing ? from 1 for char_length(?)) where data_path like ?";
#elif defined(COCKROACHDB_ICAT)
    const std::string update_filepath_on_collection_rename_sql = "update R_DATA_MAIN set data_path = overlay(data_path placing ? from 1 for ?) where data_path like ?";
#else
    const std::string update_filepath_on_collection_rename_sql = "update R_DATA_MAIN set data_path = replace(data_path, ?, ?) where data_path like ?";
#endif


// finds an irods object with the attr/val/unit combination, returns the first entry in the list
// return values:
//    1 - no rows found but no other error
//    0 - row found
//    -1 - error encountered
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
        return -2;
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

// Returns the path in irods for a file in lustre based on the mapping in register_map.  
// If the prefix is not in register_map then the function returns -1, otherwise it returns 0.
int lustre_path_to_irods_path(const std::string& lustre_path, const std::vector<std::pair<std::string, std::string> >& register_map,
        std::string& irods_path) {

    for (auto& iter : register_map) {
        const std::string& lustre_path_prefix = iter.first;
        if (lustre_path.compare(0, lustre_path_prefix.length(), lustre_path_prefix) == 0) {
            irods_path = iter.second + lustre_path.substr(lustre_path_prefix.length());
            return 0;
        }
    }

    return -1;
}

// Returns the path in lustre for a data object in irods based on the mapping in register_map.  
// If the prefix is not in register_map then the function returns -1, otherwise it returns 0.
int irods_path_to_lustre_path(const std::string& irods_path, const std::vector<std::pair<std::string, std::string> >& register_map,
        std::string& lustre_path) {

    for (auto& iter : register_map) {
        const std::string& irods_path_prefix = iter.second;
        if (irods_path.compare(0, irods_path_prefix.length(), irods_path_prefix) == 0) {
            lustre_path = iter.first + irods_path.substr(irods_path_prefix.length());
            return 0;
        }
    }

    return -1;
}


int get_user_id(rsComm_t* _comm, icatSessionStruct *icss, rodsLong_t& user_id, bool direct_db_access_flag) {

    std::vector<std::string> bindVars;
    bindVars.push_back(_comm->clientUser.userName);
    int status = cmlGetIntegerValueFromSql(get_user_id_sql.c_str(), &user_id, bindVars, icss );
    if (status != 0) {
       rodsLog(LOG_ERROR, "get_user_id: cmlGetIntegerValueFromSql return %d\n", status);
       return SYS_USER_RETRIEVE_ERR;
    }
    return 0;
}

void handle_create(const std::vector<std::pair<std::string, std::string> >& register_map, 
        const int64_t& resource_id, const std::string& resource_name, const std::string& fidstr, 
        const std::string& lustre_path, const std::string& object_name, 
        const ChangeDescriptor::ObjectTypeEnum& object_type, const std::string& parent_fidstr, const int64_t& file_size,
        rsComm_t* _comm, icatSessionStruct *icss, const rodsLong_t& user_id, bool direct_db_access_flag) {


    int status;
  
    std::string irods_path; 
    if (lustre_path_to_irods_path(lustre_path.c_str(), register_map, irods_path) < 0) {
        rodsLog(LOG_NOTICE, "Skipping entry because lustre_path [%s] is not in register_map.",
                   lustre_path.c_str()); 
        return;
    }


    if (direct_db_access_flag) { 

        // register object
       
#if defined(COCKROACHDB_ICAT)
        int seq_no = cmlGetNextSeqVal(icss);
#else
        int seq_no = cmlGetCurrentSeqVal(icss);
#endif 

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


#if !defined(COCKROACHDB_ICAT)
        status =  cmlExecuteNoAnswerSql("commit", icss);
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error committing insertion of new data_object %s.  Error is %i", fidstr.c_str(), status);
            return;
        } 
#endif

        // insert user ownership
        cllBindVars[0] = std::to_string(seq_no).c_str();
        cllBindVars[1] = std::to_string(user_id).c_str();
        cllBindVarCount = 2;
        status = cmlExecuteNoAnswerSql(insert_user_ownership_data_object_sql.c_str(), icss);
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error adding onwership to object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }

#if !defined(COCKROACHDB_ICAT)
        status =  cmlExecuteNoAnswerSql("commit", icss);
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error committing ownership of new data_object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }
#endif

        // add lustre_identifier metadata
        keyValPair_t reg_param;
        memset(&reg_param, 0, sizeof(reg_param));
        addKeyVal(&reg_param, fidstr_avu_key.c_str(), fidstr.c_str());
        status = chlAddAVUMetadata(_comm, 0, "-d", irods_path.c_str(), fidstr_avu_key.c_str(), fidstr.c_str(), "");
        rodsLog(LOG_NOTICE, "Return value from chlAddAVUMetdata = %i", status);
        if (status < 0) {
            rodsLog(LOG_ERROR, "Error adding %s metadata to object %s.  Error is %i", fidstr_avu_key.c_str(), fidstr.c_str(), status);
            return;
        }

    } else {

        dataObjInp_t dataObjInp;
        memset(&dataObjInp, 0, sizeof(dataObjInp));
        strncpy(dataObjInp.objPath, irods_path.c_str(), MAX_NAME_LEN);
        addKeyVal(&dataObjInp.condInput, FILE_PATH_KW, lustre_path.c_str());
        addKeyVal(&dataObjInp.condInput, RESC_NAME_KW, resource_name.c_str());
        addKeyVal(&dataObjInp.condInput, RESC_HIER_STR_KW, resource_name.c_str());

        status = rsPhyPathReg(_comm, &dataObjInp);
        //status = filePathReg(_comm, &dataObjInp, resource_name.c_str());
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error registering object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }

        // freeKeyValPairStruct(&dataobjInp.condInput);
 
        // add lustre_identifier metadata
        modAVUMetadataInp_t modAVUMetadataInp;
        memset(&modAVUMetadataInp, 0, sizeof(modAVUMetadataInp_t)); 
        modAVUMetadataInp.arg0 = "add";
        modAVUMetadataInp.arg1 = "-d";
        modAVUMetadataInp.arg2 = const_cast<char*>(irods_path.c_str());
        modAVUMetadataInp.arg3 = const_cast<char*>(fidstr_avu_key.c_str());
        modAVUMetadataInp.arg4 = const_cast<char*>(fidstr.c_str());
        status = rsModAVUMetadata(_comm, &modAVUMetadataInp);
        if (status < 0) {
            rodsLog(LOG_ERROR, "Error adding %s metadata to object %s.  Error is %i", fidstr_avu_key.c_str(), fidstr.c_str(), status);
            return;
        }


    }
}

void handle_batch_create(const std::vector<std::pair<std::string, std::string> >& register_map, const int64_t& resource_id,
        const std::string& resource_name, const std::vector<std::string>& fidstr_list, const std::vector<std::string>& lustre_path_list,
        const std::vector<std::string>& object_name_list, const std::vector<std::string>& parent_fidstr_list,
        const std::vector<int64_t>& file_size_list, const int64_t& maximum_records_per_sql_command, rsComm_t* _comm, icatSessionStruct *icss, 
        const rodsLong_t& user_id, bool set_metadata_for_storage_tiering_time_violation, const std::string& metadata_key_for_storage_tiering_time_violation) {

    size_t insert_count = fidstr_list.size();
    int status;

    if (insert_count == 0) {
        return;
    }

    if (lustre_path_list.size() != insert_count || object_name_list.size() != insert_count ||
            parent_fidstr_list.size() != insert_count || file_size_list.size() != insert_count) {

        rodsLog(LOG_ERROR, "Handle batch create.  Received lists of differing size");
        return;
    }

    std::vector<rodsLong_t> data_obj_sequences;
    std::vector<rodsLong_t> metadata_sequences;
    cmlGetNSeqVals(icss, insert_count, data_obj_sequences);

    if (set_metadata_for_storage_tiering_time_violation) {
        cmlGetNSeqVals(icss, insert_count+1, metadata_sequences);
    } else {
        cmlGetNSeqVals(icss, insert_count, metadata_sequences);
    }

    // insert into R_DATA_MAIN
 
    std::string insert_sql(200 + insert_count*140, 0);
    insert_sql = "insert into R_DATA_MAIN (data_id, coll_id, data_name, data_repl_num, data_type_name, "
                             "data_size, resc_name, data_path, data_owner_name, data_owner_zone, data_is_dirty, data_map_id, resc_id) "
                        "values ";

    // cache the collection id's from parent_fidstr
    std::map<std::string, rodsLong_t> fidstr_to_collection_id_map;

    for (size_t i = 0; i < insert_count; ++i) {

        rodsLong_t coll_id;

        auto iter = fidstr_to_collection_id_map.find(parent_fidstr_list[i]);

        if (iter != fidstr_to_collection_id_map.end()) {
            coll_id = iter->second;
        } else {
            std::vector<std::string> bindVars;
            bindVars.push_back(parent_fidstr_list[i]);
            status = cmlGetIntegerValueFromSql(get_collection_id_from_fidstr_sql.c_str(), &coll_id, bindVars, icss );
            if (status != 0) {
                rodsLog(LOG_ERROR, "Error during registration object %s.  Error getting collection id for collection with fidstr=%s.  Error is %i", 
                        fidstr_list[i].c_str(), parent_fidstr_list[i].c_str(), status);
                continue;
            }

            fidstr_to_collection_id_map[parent_fidstr_list[i]] = coll_id;
        }

        insert_sql += "(" + std::to_string(data_obj_sequences[i]) + ", " + std::to_string(coll_id) + ", '" + object_name_list[i] + "', " +
            std::to_string(0) +  ", 'generic', " + std::to_string(file_size_list[i]) + ", 'EMPTY_RESC_NAME', '" + lustre_path_list[i] + "', '" + 
            _comm->clientUser.userName + "', '" + _comm->clientUser.rodsZone + "', 0, 0, " + std::to_string(resource_id) + ")";

        if (i < insert_count - 1) {
            insert_sql += ", ";
        }
    }

    cllBindVarCount = 0;
    status = cmlExecuteNoAnswerSql(insert_sql.c_str(), icss);
    if (status != 0) {
        rodsLog(LOG_ERROR, "Error performing batch insert of objects.  Error is %i.  SQL is %s.", status, insert_sql.c_str());
        return;
    }

#if !defined(COCKROACHDB_ICAT)
    status =  cmlExecuteNoAnswerSql("commit", icss);
    if (status != 0) {
        rodsLog(LOG_ERROR, "Error committing insert into R_META_MAIN.  Error is %i", status);
        return;
    } 
#endif

    // Insert into R_META_MAIN
    
    insert_sql = "insert into R_META_MAIN (meta_id, meta_attr_name, meta_attr_value) values ";

    for (size_t i = 0; i < insert_count; ++i) {
        insert_sql += "(" + std::to_string(metadata_sequences[i]) + ", '" + fidstr_avu_key + "', '" + 
            fidstr_list[i] + "')";

        if (i < insert_count - 1) {
            insert_sql += ", ";
        }
    }

    cllBindVarCount = 0;
    status = cmlExecuteNoAnswerSql(insert_sql.c_str(), icss);
    if (status != 0) {
        rodsLog(LOG_ERROR, "Error performing batch insert into R_META_MAIN.  Error is %i.  SQL is %s.", status, insert_sql.c_str());
        return;
    }

#if !defined(COCKROACHDB_ICAT)
    status =  cmlExecuteNoAnswerSql("commit", icss);
    if (status != 0) {
        rodsLog(LOG_ERROR, "Error committing insert into R_META_MAIN.  Error is %i", status);
        return;
    } 
#endif

    // if we are setting the access time metadata for storage tiering
    if (set_metadata_for_storage_tiering_time_violation) {
        // Insert access time into R_META_MAIN

        time_t now = time(NULL);
	    
        insert_sql = "insert into R_META_MAIN (meta_id, meta_attr_name, meta_attr_value) values (" +
                     std::to_string(metadata_sequences[insert_count])  + ", '" + 
                     metadata_key_for_storage_tiering_time_violation + "', '" +
                     std::to_string(now) + "')";

	cllBindVarCount = 0;
	status = cmlExecuteNoAnswerSql(insert_sql.c_str(), icss);
	if (status != 0) {
	    rodsLog(LOG_ERROR, "Error inserting metadata into R_META_MAIN for %s.  Error is %i.  SQL is %s.", 
                    metadata_key_for_storage_tiering_time_violation.c_str(), status, insert_sql.c_str());
	    return;
	}

#if !defined(COCKROACHDB_ICAT)
        status =  cmlExecuteNoAnswerSql("commit", icss);
        if (status != 0) {
   	    rodsLog(LOG_ERROR, "Error committing insert into R_META_MAIN.  Error is %i", status);
	    return;
	} 
#endif

    } // set_metadata_for_storage_tiering_time_violation


    // Insert into R_OBJT_METMAP

    insert_sql = "insert into R_OBJT_METAMAP (object_id, meta_id) values ";

    for (size_t i = 0; i < insert_count; ++i) {
        insert_sql += "(" + std::to_string(data_obj_sequences[i]) + ", " + std::to_string(metadata_sequences[i]) + ")";

        if (i < insert_count - 1) {
            insert_sql += ", ";
        }
    }
 
    cllBindVarCount = 0;
    status = cmlExecuteNoAnswerSql(insert_sql.c_str(), icss);
    if (status != 0) {
        rodsLog(LOG_ERROR, "Error performing batch insert into R_OBJT_METAMAP.  Error is %i.  SQL is %s.", status, insert_sql.c_str());
        return;
    }

#if !defined(COCKROACHDB_ICAT)
    status =  cmlExecuteNoAnswerSql("commit", icss);
    if (status != 0) {
        rodsLog(LOG_ERROR, "Error committing insert into R_OBJT_METAMAP.  Error is %i", status);
        return;
    } 
#endif

    
    // if we are setting the access time metadata for storage tiering
    if (set_metadata_for_storage_tiering_time_violation) {

        insert_sql = "insert into R_OBJT_METAMAP (object_id, meta_id) values ";

        for (size_t i = 0; i < insert_count; ++i) {
            insert_sql += "(" + std::to_string(data_obj_sequences[i]) + ", " + std::to_string(metadata_sequences[insert_count]) + ")";

            if (i < insert_count - 1) {
                insert_sql += ", ";
            }
        }
 
        cllBindVarCount = 0;
        status = cmlExecuteNoAnswerSql(insert_sql.c_str(), icss);
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error performing batch insert into R_OBJT_METAMAP.  Error is %i.  SQL is %s.", status, insert_sql.c_str());
            return;
        }

#if !defined(COCKROACHDB_ICAT)
        status =  cmlExecuteNoAnswerSql("commit", icss);
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error committing insert into R_OBJT_METAMAP for storage tiering time metadata.  Error is %i", status);
            return;
        } 
#endif

    } // set_metadata_for_storage_tiering_time_violation


    // insert user ownership
    //insert into R_OBJT_ACCESS (object_id, user_id, access_type_id) values (?, ?, 1200) 
    insert_sql = "insert into R_OBJT_ACCESS (object_id, user_id, access_type_id) values ";

    for (size_t i = 0; i < insert_count; ++i) {
        insert_sql += "(" + std::to_string(data_obj_sequences[i]) + ", " + std::to_string(user_id) + ", 1200)";

        if (i < insert_count - 1) {
            insert_sql += ", ";
        }
    }
 
    cllBindVarCount = 0;
    status = cmlExecuteNoAnswerSql(insert_sql.c_str(), icss);
    if (status != 0) {
        rodsLog(LOG_ERROR, "Error performing batch insert into R_OBJT_ACCESS.  Error is %i.  SQL is %s.", status, insert_sql.c_str());
        return;
    }

#if !defined(COCKROACHDB_ICAT)
    status =  cmlExecuteNoAnswerSql("commit", icss);
    if (status != 0) {
        rodsLog(LOG_ERROR, "Error committing insert into R_OBJT_ACCESS.  Error is %i", status);
        return;
    } 
#endif
}


void handle_mkdir(const std::vector<std::pair<std::string, std::string> >& register_map, 
        const int64_t& resource_id, const std::string& resource_name, const std::string& fidstr, 
        const std::string& lustre_path, const std::string& object_name, 
        const ChangeDescriptor::ObjectTypeEnum& object_type, const std::string& parent_fidstr, const int64_t& file_size,
        rsComm_t* _comm, icatSessionStruct *icss, const rodsLong_t& user_id, bool direct_db_access_flag) {


    int status;

    std::string irods_path;
    if (lustre_path_to_irods_path(lustre_path, register_map, irods_path) < 0) {
        rodsLog(LOG_NOTICE, "Skipping mkdir on lustre_path [%s] which is not in register_map.",
               lustre_path.c_str());
        return;
    }

    if (direct_db_access_flag) { 

        collInfo_t coll_info;
        memset(&coll_info, 0, sizeof(coll_info));
        strncpy(coll_info.collName, irods_path.c_str(), MAX_NAME_LEN);

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
        status = chlAddAVUMetadata(_comm, 0, "-C", irods_path.c_str(), fidstr_avu_key.c_str(), fidstr.c_str(), "");
        rodsLog(LOG_NOTICE, "Return value from chlAddAVUMetadata = %i", status);
        if (status < 0) {
            rodsLog(LOG_ERROR, "Error adding %s metadata to object %s.  Error is %i", fidstr_avu_key.c_str(), fidstr.c_str(), status);
            return;
        }

    } else {


        // register object
        collInp_t coll_input;
        memset(&coll_input, 0, sizeof(coll_input));
        strncpy(coll_input.collName, irods_path.c_str(), MAX_NAME_LEN);
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
        modAVUMetadataInp.arg2 = const_cast<char*>(irods_path.c_str());
        modAVUMetadataInp.arg3 = const_cast<char*>(fidstr_avu_key.c_str());
        modAVUMetadataInp.arg4 = const_cast<char*>(fidstr.c_str());
        status = rsModAVUMetadata(_comm, &modAVUMetadataInp);
        if (status < 0) {
            rodsLog(LOG_ERROR, "Error adding %s metadata to object %s.  Error is %i", fidstr_avu_key.c_str(), fidstr.c_str(), status);
            return;
        }


    }

}

void handle_other(const std::vector<std::pair<std::string, std::string> >& register_map, 
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

#if !defined(COCKROACHDB_ICAT)
        status =  cmlExecuteNoAnswerSql("commit", icss);

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error committing update to data_object_size for data_object %s.  Error is %i", fidstr.c_str(), status);
            return;
        } 
#endif

    } else {

        std::string irods_path;
       
        // look up object based on fidstr
        status = find_irods_path_with_avu(_comm, fidstr_avu_key, fidstr, "", false, irods_path); 

        // modify the file size
        modDataObjMeta_t modDataObjMetaInp;
        dataObjInfo_t dataObjInfo;
        memset( &modDataObjMetaInp, 0, sizeof( modDataObjMetaInp ) );
        memset( &dataObjInfo, 0, sizeof( dataObjInfo ) );

        keyValPair_t reg_param;
        memset( &reg_param, 0, sizeof( reg_param ) );
        char tmpStr[MAX_NAME_LEN];
        snprintf( tmpStr, sizeof( tmpStr ), "%ji", ( intmax_t ) file_size );
        addKeyVal( &reg_param, DATA_SIZE_KW, tmpStr );
        modDataObjMetaInp.regParam = &reg_param;


        modDataObjMetaInp.dataObjInfo = &dataObjInfo;
        dataObjInfo.dataSize = file_size; 
        strncpy(dataObjInfo.filePath, lustre_path.c_str(), MAX_NAME_LEN);
        strncpy(dataObjInfo.objPath, irods_path.c_str(), MAX_NAME_LEN);

rodsLog(LOG_ERROR, "rsModDataObjMeta( %p, %p )", _comm, &modDataObjMetaInp);
rodsLog(LOG_ERROR, "modDataObjMetaInp.dataObjInfo->dataSize = %d", modDataObjMetaInp.dataObjInfo->dataSize);
rodsLog(LOG_ERROR, "modDataObjMetaInp.dataObjInfo->filePath = %s", modDataObjMetaInp.dataObjInfo->filePath);
rodsLog(LOG_ERROR, "modDataObjMetaInp.dataObjInfo->objPath = %s", modDataObjMetaInp.dataObjInfo->objPath);
rodsLog(LOG_ERROR, "modDataObjMetaInp.regParam->len= %d", modDataObjMetaInp.regParam->len);
        status = rsModDataObjMeta( _comm, &modDataObjMetaInp );

        if ( status < 0 ) {
            rodsLog(LOG_ERROR, "Error updating data object rename for data_object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }

    }

}

void handle_rename_file(const std::vector<std::pair<std::string, std::string> >& register_map, 
        const int64_t& resource_id, const std::string& resource_name, const std::string& fidstr, 
        const std::string& lustre_path, const std::string& object_name, 
        const ChangeDescriptor::ObjectTypeEnum& object_type, const std::string& parent_fidstr, const int64_t& file_size,
        rsComm_t* _comm, icatSessionStruct *icss, const rodsLong_t& user_id, bool direct_db_access_flag) {

rodsLog(LOG_ERROR, "lustre_path: %s", lustre_path.c_str());

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

#if !defined(COCKROACHDB_ICAT)
        status =  cmlExecuteNoAnswerSql("commit", icss);

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error committing update to data object rename for data_object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }
#endif
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

        // modify the file path
        modDataObjMeta_t modDataObjMetaInp;
        dataObjInfo_t dataObjInfo;
        memset( &modDataObjMetaInp, 0, sizeof( modDataObjMetaInp ) );
        memset( &dataObjInfo, 0, sizeof( dataObjInfo ) );

        keyValPair_t reg_param;
        memset( &reg_param, 0, sizeof( reg_param ) );
        addKeyVal( &reg_param, FILE_PATH_KW, lustre_path.c_str());
        modDataObjMetaInp.regParam = &reg_param;

        modDataObjMetaInp.dataObjInfo = &dataObjInfo;
        strncpy(dataObjInfo.objPath, old_irods_path.c_str(), MAX_NAME_LEN);

        status = rsModDataObjMeta( _comm, &modDataObjMetaInp );

        if ( status < 0 ) {
            rodsLog(LOG_ERROR, "Error updating data object rename for data_object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }

        // rename the data object 
        // Note:  rsModDataObjMeta does not have an option to update the object path 
        dataObjCopyInp_t dataObjRenameInp;
        memset( &dataObjRenameInp, 0, sizeof( dataObjRenameInp ) );

        strncpy(dataObjRenameInp.srcDataObjInp.objPath, old_irods_path.c_str(), MAX_NAME_LEN);
        strncpy(dataObjRenameInp.destDataObjInp.objPath, new_irods_path.c_str(), MAX_NAME_LEN);
        dataObjRenameInp.srcDataObjInp.oprType = dataObjRenameInp.destDataObjInp.oprType = RENAME_DATA_OBJ;
        addKeyVal(&dataObjRenameInp.srcDataObjInp.condInput, FORCE_FLAG_KW, "");
        addKeyVal(&dataObjRenameInp.destDataObjInp.condInput, FORCE_FLAG_KW, "");
        status = rsDataObjRename(_comm, &dataObjRenameInp);

        if ( status < 0 ) {
            rodsLog(LOG_ERROR, "Error updating data object rename for data_object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }

    }

}

void handle_rename_dir(const std::vector<std::pair<std::string, std::string> >& register_map, 
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


    // until issue 6 is resolved, just do all directory renames as direct db access 
    if (true) {
    //if (direct_db_access_flag) { 

        char parent_path_cstr[MAX_NAME_LEN];
        std::string collection_path;

        // get the parent's path - using parent's fidstr
        std::vector<std::string> bindVars;
        bindVars.push_back(parent_fidstr);
rodsLog(LOG_ERROR, "%s:%i - %s()", __FILE__, __LINE__, __FUNCTION__);
rodsLog(LOG_ERROR, "parent_fidstr=%s", parent_fidstr.c_str());
rodsLog(LOG_ERROR, "cmlGetStringValueFromSql(%s, parent_path_cstr, %d, bindVars[%s], icss)", get_collection_path_from_fidstr_sql.c_str(), MAX_NAME_LEN, parent_fidstr.c_str());
        status = cmlGetStringValueFromSql(get_collection_path_from_fidstr_sql.c_str(), parent_path_cstr, MAX_NAME_LEN, bindVars, icss);
rodsLog(LOG_ERROR, "%s:%i - %s()", __FILE__, __LINE__, __FUNCTION__);
        std::string parent_path(parent_path_cstr);
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error looking up parent collection for rename for collection %s.  Error is %i", fidstr.c_str(), status);
            cmlExecuteNoAnswerSql("rollback", icss);
        }

        collection_path = parent_path + irods::get_virtual_path_separator().c_str() + object_name;

        rodsLog(LOG_DEBUG, "collection path = %s\tparent_path = %s", collection_path.c_str(), parent_path.c_str());
          

        // update coll_name, parent_coll_name, and coll_id
        cllBindVars[0] = collection_path.c_str();
        cllBindVars[1] = parent_path.c_str();
        cllBindVars[2] = fidstr.c_str();
        cllBindVarCount = 3;
        status = cmlExecuteNoAnswerSql(update_collection_for_rename_sql.c_str(), icss);

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error updating collection object rename for collection %s.  Error is %i", fidstr.c_str(), status);
            cmlExecuteNoAnswerSql("rollback", icss);
            return;
        }

#if !defined(COCKROACHDB_ICAT)
        status =  cmlExecuteNoAnswerSql("commit", icss);

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error committing update to collection rename for collection %s.  Error is %i", fidstr.c_str(), status);
            return;
        }
#endif

        try {


            //std::string old_lustre_path = lustre_root_path + old_irods_path.substr(register_path.length());
            //std::string new_lustre_path = lustre_root_path + new_irods_path.substr(register_path.length());
        
            std::string old_lustre_path;   
            std::string new_lustre_path;   

            if (irods_path_to_lustre_path(old_irods_path, register_map, old_lustre_path) < 0) {
                rodsLog(LOG_ERROR, "%s - could not convert old irods path [%s] to old lustre path .  skipping.\n", old_irods_path.c_str(), old_lustre_path.c_str());
                return;
            }

            if (irods_path_to_lustre_path(new_irods_path, register_map, new_lustre_path) < 0) {
                rodsLog(LOG_ERROR, "%s - could not convert new irods path [%s] to new lustre path .  skipping.\n", new_irods_path.c_str(), new_lustre_path.c_str());
                return;
            }

            std::string like_clause = old_lustre_path + "/%";

            rodsLog(LOG_DEBUG, "old_lustre_path = %s", old_lustre_path.c_str());
            rodsLog(LOG_DEBUG, "new_lustre_path = %s", new_lustre_path.c_str());

            // for now, rename all with sql update
#if defined(POSTGRES_ICAT)
            cllBindVars[0] = new_lustre_path.c_str();
            cllBindVars[1] = old_lustre_path.c_str();
            cllBindVars[2] = like_clause.c_str();
            cllBindVarCount = 3;
            status = cmlExecuteNoAnswerSql(update_filepath_on_collection_rename_sql.c_str(), icss);
#elif defined(COCKROACHDB_ICAT)
            cllBindVars[0] = new_lustre_path.c_str();
            std::string old_path_len_str = std::to_string(old_lustre_path.length());
            cllBindVars[1] = old_path_len_str.c_str();
            cllBindVars[2] = like_clause.c_str();
            cllBindVarCount = 3;
            status = cmlExecuteNoAnswerSql(update_filepath_on_collection_rename_sql.c_str(), icss);
#else
            // oracle and mysql
            cllBindVars[0] = old_lustre_path.c_str();
            cllBindVars[1] = new_lustre_path.c_str();
            cllBindVars[2] = like_clause.c_str();
            cllBindVarCount = 3;
            status = cmlExecuteNoAnswerSql(update_filepath_on_collection_rename_sql.c_str(), icss);
#endif

            if ( status < 0 && status != CAT_SUCCESS_BUT_WITH_NO_INFO) {
                rodsLog(LOG_ERROR, "Error updating data objects after collection move for collection %s.  Error is %i", fidstr.c_str(), status);
                cmlExecuteNoAnswerSql("rollback", icss);
                return;
            }

#if !defined(COCKROACHDB_ICAT)
            status =  cmlExecuteNoAnswerSql("commit", icss);
            if (status != 0) {
                rodsLog(LOG_ERROR, "Error committing data object update after collection move for collection %s.  Error is %i", fidstr.c_str(), status);
                return;
            } 
#endif

        } catch(const std::out_of_range& e) {
            rodsLog(LOG_ERROR, "Error updating data objects after collection move for collection %s.  Error is %i", fidstr.c_str(), status);
            return;
        }


    } else {

        // rename the data object 
        dataObjCopyInp_t dataObjRenameInp;
        memset( &dataObjRenameInp, 0, sizeof( dataObjRenameInp ) );

        strncpy(dataObjRenameInp.srcDataObjInp.objPath, old_irods_path.c_str(), MAX_NAME_LEN);
        strncpy(dataObjRenameInp.destDataObjInp.objPath, new_irods_path.c_str(), MAX_NAME_LEN);
        dataObjRenameInp.srcDataObjInp.oprType = dataObjRenameInp.destDataObjInp.oprType = RENAME_COLL;

        status = rsDataObjRename( _comm, &dataObjRenameInp );
        if ( status < 0 ) {
            rodsLog(LOG_ERROR, "Error updating data object rename for data_object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }

        // TODO: Issue 6 - Handle update of data object physical paths using iRODS API's 

    }

}

void handle_unlink(const std::vector<std::pair<std::string, std::string> >& register_map, 
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


#if !defined(COCKROACHDB_ICAT)
        status =  cmlExecuteNoAnswerSql("commit", icss);

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error committing delete for data object %s.  Error is %i", fidstr.c_str(), status);
            return;
        }
#endif

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

#if !defined(COCKROACHDB_ICAT)

    void handle_batch_unlink(const std::vector<std::string>& fidstr_list, const int64_t& maximum_records_per_sql_command, rsComm_t* _comm, icatSessionStruct *icss) {

        //size_t transactions_per_update = 1;
        int64_t delete_count = fidstr_list.size();
        int status;

        // delete from R_DATA_MAIN

        std::string query_objects_sql(220 + maximum_records_per_sql_command*20, 0); 
        std::string delete_sql(50 + maximum_records_per_sql_command*20, 0);

        // Do deletion in batches of size maximum_records_per_sql_command.  
        
        // batch_begin is start of current batch
        int64_t batch_begin = 0;
        while (batch_begin < delete_count) {

        // Build up a list of object id's to be deleted on this pass
        // Note:  Doing this in code rather than in a subquery seems to free up DB processing and
        //   also resolves deadlock potential.

        std::vector<std::string> object_id_list;

        std::string query_objects_sql = "select R_OBJT_METAMAP.object_id from R_OBJT_METAMAP inner join R_META_MAIN on R_META_MAIN.meta_id = R_OBJT_METAMAP.meta_id "
                        "where R_META_MAIN.meta_attr_name = 'lustre_identifier' and R_META_MAIN.meta_attr_value in (";
         
        for (int64_t i = 0; batch_begin + i < delete_count && i < maximum_records_per_sql_command; ++i) {
            query_objects_sql += "'" + fidstr_list[batch_begin + i] + "'";
            if (batch_begin + i == delete_count - 1 || maximum_records_per_sql_command - 1 == i) {
            query_objects_sql += ")";
            } else {
            query_objects_sql += ", ";
            }
        }

        std::vector<std::string> emptyBindVars;
        int stmt_num;
        status = cmlGetFirstRowFromSqlBV(query_objects_sql.c_str(), emptyBindVars, &stmt_num, icss);
         if ( status < 0 ) {
            rodsLog(LOG_ERROR, "retrieving object for unlink - query %s, failure %d", query_objects_sql.c_str(), status);
            cllFreeStatement(icss, stmt_num);
            return;
        }

        size_t nCols = icss->stmtPtr[stmt_num]->numOfCols;

        if (nCols != 1) {
            rodsLog(LOG_ERROR, "cmlGetFirstRowFromSqlBV for query %s, unexpected number of columns %d", query_objects_sql.c_str(), nCols);
            cllFreeStatement(icss, stmt_num);
            return;
        }

        object_id_list.push_back(icss->stmtPtr[stmt_num]->resultValue[0]);

        while (cmlGetNextRowFromStatement( stmt_num, icss ) == 0) {

            object_id_list.push_back(icss->stmtPtr[stmt_num]->resultValue[0]);
        }

        cllFreeStatement(icss, stmt_num);


        // Now do the delete
        
        delete_sql = "delete from R_DATA_MAIN where data_id in (";
        for (size_t i = 0; i < object_id_list.size(); ++i) {
            delete_sql += object_id_list[i];
            if (i == object_id_list.size() - 1) {
                delete_sql += ")";
            } else {
                delete_sql += ", ";
            }
        }

        rodsLog(LOG_DEBUG, "delete sql is %s", delete_sql.c_str());

        cllBindVarCount = 0;
        status = cmlExecuteNoAnswerSql(delete_sql.c_str(), icss);
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error performing batch delete from R_DATA_MAIN.  Error is %i.  SQL is %s.", status, delete_sql.c_str());
            return;
        }

        status =  cmlExecuteNoAnswerSql("commit", icss);
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error committing batched deletion from R_DATA_MAIN.  Error is %i", status);
            return;
        }

        // delete from R_OBJT_METAMAP

        delete_sql = "delete from R_OBJT_METAMAP where object_id in (";
        for (size_t i = 0; i < object_id_list.size(); ++i) {
            delete_sql += object_id_list[i];
            if (i == object_id_list.size() - 1) {
            delete_sql += ")";
            } else {
            delete_sql += ", ";
            }
        }

        rodsLog(LOG_DEBUG, "delete sql is %s", delete_sql.c_str());

        cllBindVarCount = 0;
        status = cmlExecuteNoAnswerSql(delete_sql.c_str(), icss);
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error performing batch delete from R_DATA_MAIN.  Error is %i.  SQL is %s.", status, delete_sql.c_str());
            return;
        }

        status =  cmlExecuteNoAnswerSql("commit", icss);
        if (status != 0) {
            rodsLog(LOG_ERROR, "Error committing batched deletion of data objects.  Error is %i", status);
            return;
        }


        batch_begin += maximum_records_per_sql_command;

        }

    }

#endif // !defined(COCKROACHDB_ICAT)

#if defined(COCKROACHDB_ICAT)

    void handle_batch_unlink(const std::vector<std::string>& fidstr_list, const int64_t& maximum_records_per_sql_command, rsComm_t* _comm, icatSessionStruct *icss) {

        //size_t transactions_per_update = 1;
        int64_t delete_count = fidstr_list.size();
        int status;

        // delete from R_DATA_MAIN

        std::string query_objects_sql(220 + maximum_records_per_sql_command*20, 0); 
        std::string delete_sql(50 + maximum_records_per_sql_command*20, 0);

        // Do deletion in batches of size maximum_records_per_sql_command.  
        
        // batch_begin is start of current batch
        int64_t batch_begin = 0;
        while (batch_begin < delete_count) {

            // Build up a list of object id's to be deleted on this pass
            // Note:  Doing this in code rather than in a subquery seems to free up DB processing and
            //   also resolves deadlock potential.

            std::vector<std::string> object_id_list;

            std::string query_objects_sql = "select R_OBJT_METAMAP.object_id from R_OBJT_METAMAP inner join R_META_MAIN on R_META_MAIN.meta_id = R_OBJT_METAMAP.meta_id "
                                            "where R_META_MAIN.meta_attr_name = 'lustre_identifier' and R_META_MAIN.meta_attr_value in (";
         
            for (int64_t i = 0; batch_begin + i < delete_count && i < maximum_records_per_sql_command; ++i) {
                query_objects_sql += "'" + fidstr_list[batch_begin + i] + "'";
                if (batch_begin + i == delete_count - 1 || maximum_records_per_sql_command - 1 == i) {
                    query_objects_sql += ")";
                } else {
                    query_objects_sql += ", ";
                }
            }

            std::vector<std::string> emptyBindVars;
            int stmt_num;
            status = cmlGetFirstRowFromSqlBV(query_objects_sql.c_str(), emptyBindVars, &stmt_num, icss);
             if ( status < 0 ) {
                rodsLog(LOG_ERROR, "retrieving object for unlink - query %s, failure %d", query_objects_sql.c_str(), status);
                cllFreeStatement(stmt_num);
                return;
            }

            
            size_t nCols = result_sets[stmt_num]->row_size();

            if (nCols != 1) {
                rodsLog(LOG_ERROR, "cmlGetFirstRowFromSqlBV for query %s, unexpected number of columns %d", query_objects_sql.c_str(), nCols);
                cllFreeStatement(stmt_num);
                return;
            }

            object_id_list.push_back(result_sets[stmt_num]->get_value(0));

            while (cmlGetNextRowFromStatement( stmt_num, icss ) == 0) {

                object_id_list.push_back(result_sets[stmt_num]->get_value(0));
            }

            cllFreeStatement(stmt_num);

            // Now do the delete
            
            delete_sql = "delete from R_DATA_MAIN where data_id in (";
            for (size_t i = 0; i < object_id_list.size(); ++i) {
                delete_sql += object_id_list[i];
                if (i == object_id_list.size() - 1) {
                    delete_sql += ")";
                } else {
                    delete_sql += ", ";
                }
            }

            rodsLog(LOG_DEBUG, "delete sql is %s", delete_sql.c_str());

            cllBindVarCount = 0;
            status = cmlExecuteNoAnswerSql(delete_sql.c_str(), icss);
            if (status != 0) {
                rodsLog(LOG_ERROR, "Error performing batch delete from R_DATA_MAIN.  Error is %i.  SQL is %s.", status, delete_sql.c_str());
                return;
            }

//            status =  cmlExecuteNoAnswerSql("commit", icss);
//            if (status != 0) {
//                rodsLog(LOG_ERROR, "Error committing batched deletion from R_DATA_MAIN.  Error is %i", status);
//                return;
//            }


            // delete from R_OBJT_METAMAP

            delete_sql = "delete from R_OBJT_METAMAP where object_id in (";
            for (size_t i = 0; i < object_id_list.size(); ++i) {
                delete_sql += object_id_list[i];
                if (i == object_id_list.size() - 1) {
                    delete_sql += ")";
                } else {
                    delete_sql += ", ";
                }
            }

            rodsLog(LOG_DEBUG, "delete sql is %s", delete_sql.c_str());

            cllBindVarCount = 0;
            status = cmlExecuteNoAnswerSql(delete_sql.c_str(), icss);
            if (status != 0) {
                rodsLog(LOG_ERROR, "Error performing batch delete from R_DATA_MAIN.  Error is %i.  SQL is %s.", status, delete_sql.c_str());
                return;
            }

//            status =  cmlExecuteNoAnswerSql("commit", icss);
//            if (status != 0) {
//                rodsLog(LOG_ERROR, "Error committing batched deletion of data objects.  Error is %i", status);
//                return;
//            }

            batch_begin += maximum_records_per_sql_command;

        }

    }
#endif // defined(COCKROACHDB_ICAT)

void handle_rmdir(const std::vector<std::pair<std::string, std::string> >& register_map, 
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

#if !defined(COCKROACHDB_ICAT)
        status =  cmlExecuteNoAnswerSql("commit", icss);

        if (status != 0) {
            rodsLog(LOG_ERROR, "Error committing delete for collection %s.  Error is %i", fidstr.c_str(), status);
            return;
        }
#endif

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

void handle_write_fid(const std::vector<std::pair<std::string, std::string> >& register_map, const std::string& lustre_path, 
                const std::string& fidstr, rsComm_t* _comm, icatSessionStruct *icss, bool direct_db_access_flag) {

    std::string irods_path;
    if (lustre_path_to_irods_path(lustre_path, register_map, irods_path) < 0) {
        rodsLog(LOG_NOTICE, "Skipping handle_write_fid on lustre_path [%s] which is not in register_map.",
               lustre_path.c_str());
        return;
    }

    // query metadata to see if it already exists
    if (direct_db_access_flag) {
        rodsLong_t coll_id;
        std::vector<std::string> bindVars;
        bindVars.push_back(fidstr);
        if (cmlGetIntegerValueFromSql(get_collection_id_from_fidstr_sql.c_str(), &coll_id, bindVars, icss ) != CAT_NO_ROWS_FOUND) {
            return;
        }
    } else {
        int status = find_irods_path_with_avu(_comm, fidstr_avu_key, fidstr, "", true, irods_path); 
        if (status == 0) {
            // found a row which means the avu is already there, just return     
            return;
        } 
    }

    // add lustre_identifier metadata
    modAVUMetadataInp_t modAVUMetadataInp;
    memset(&modAVUMetadataInp, 0, sizeof(modAVUMetadataInp_t)); 
    modAVUMetadataInp.arg0 = "add";
    modAVUMetadataInp.arg1 = "-C";
    modAVUMetadataInp.arg2 = const_cast<char*>(irods_path.c_str());
    modAVUMetadataInp.arg3 = const_cast<char*>(fidstr_avu_key.c_str());
    modAVUMetadataInp.arg4 = const_cast<char*>(fidstr.c_str());

    // ignore error code because the fid metadata likely already exists on the root collection
    rsModAVUMetadata(_comm, &modAVUMetadataInp);

}


