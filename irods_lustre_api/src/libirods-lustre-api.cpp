// =-=-=-=-=-=-=-
// irods includes
#include "apiHandler.hpp"
#include "irods_stacktrace.hpp"
#include "irods_server_api_call.hpp"
#include "irods_re_serialization.hpp"
#include "objStat.h"
#include "icatHighLevelRoutines.hpp"

#include "boost/lexical_cast.hpp"

// =-=-=-=-=-=-=-
// stl includes
#include <sstream>
#include <string>
#include <iostream>

// json header
#include <jeayeson/jeayeson.hpp>

#include "inout_structs.h"

int cmlExecuteNoAnswerSql( const char *sql, icatSessionStruct *icss );

#define MAX_BIND_VARS 32000
extern const char *cllBindVars[MAX_BIND_VARS];
extern int cllBindVarCount;

const char *update_data_size_sql = "update R_DATA_MAIN set data_size = ? where data_id = ("
                   "select r_data_main.data_id "
                   "from r_data_main "
                   "inner join r_objt_metamap on r_data_main.data_id = r_objt_metamap.object_id "
                   "inner join r_meta_main on r_meta_main.meta_id = r_objt_metamap.meta_id "
                   "where r_meta_main.meta_attr_name = 'fidstr' and r_meta_main.meta_attr_value = ?)";

const char *update_data_object_for_rename_sql = "update R_DATA_MAIN set data_name = ?, data_path = ?, coll_id = ("
                   "select r_coll_main.coll_id "
                   "from r_coll_main "
                   "inner join r_objt_metamap on r_coll_main.coll_id = r_objt_metamap.object_id "
                   "inner join r_meta_main on r_meta_main.meta_id = r_objt_metamap.meta_id "
                   "where r_meta_main.meta_attr_name = 'fidstr' and r_meta_main.meta_attr_value = ?) "
                   "where data_id = ("
                   "select r_data_main.data_id "
                   "from r_data_main "
                   "inner join r_objt_metamap on r_data_main.data_id = r_objt_metamap.object_id "
                   "inner join r_meta_main on r_meta_main.meta_id = r_objt_metamap.meta_id "
                   "where r_meta_main.meta_attr_name = 'fidstr' and r_meta_main.meta_attr_value = ?)";

//const char *update_collection_for_rename_sql = "update R_COLL_MAIN set coll_name = ?, parent_coll_name



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

std::string read_string_value_from_json_map(const std::string& key, const json_map& m) {
    std::stringstream tmp;
    tmp << m.find(key)->second; 
    return tmp.str();
}


int call_irodsLustreApiInp_irodsLustreApiOut( irods::api_entry* _api, 
                            rsComm_t*  _comm,
                            irodsLustreApiInp_t* _inp, 
                            irodsLustreApiOut_t** _out ) {
    return _api->call_handler<
               irodsLustreApiInp_t*,
               irodsLustreApiOut_t** >(
                   _comm,
                   _inp,
                   _out );
}

#ifdef RODS_SERVER
static irods::error serialize_irodsLustreApiInp_ptr( boost::any _p, 
                                            irods::re_serialization::serialized_parameter_t& _out) {
    try {
        irodsLustreApiInp_t* tmp = boost::any_cast<irodsLustreApiInp_t*>(_p);
        if(tmp) {
            _out["change_log_json"] = boost::lexical_cast<std::string>(tmp->change_log_json);
        }
        else {
            _out["change_log_json"] = "";
        }
    }
    catch ( std::exception& ) {
        return ERROR(
                INVALID_ANY_CAST,
                "failed to cast irodsLustreApiInp_t ptr" );
    }

    return SUCCESS();
} // serialize_irodsLustreApiInp_ptr

static irods::error serialize_irodsLustreApiOut_ptr_ptr( boost::any _p,
                                                irods::re_serialization::serialized_parameter_t& _out) {
    try {
        irodsLustreApiOut_t** tmp = boost::any_cast<irodsLustreApiOut_t**>(_p);
        if(tmp && *tmp ) {
            irodsLustreApiOut_t*  l = *tmp;
            _out["status"] = boost::lexical_cast<std::string>(l->status);
        }
        else {
            _out["status"] = -1;
        }
    }
    catch ( std::exception& ) {
        return ERROR(
                INVALID_ANY_CAST,
                "failed to cast irodsLustreApiOut_t ptr" );
    }

    return SUCCESS();
} // serialize_irodsLustreApiOut_ptr_ptr
#endif


#ifdef RODS_SERVER
    #define CALL_IRODS_LUSTRE_API_INP_OUT call_irodsLustreApiInp_irodsLustreApiOut 
#else
    #define CALL_IRODS_LUSTRE_API_INP_OUT NULL 
#endif

// =-=-=-=-=-=-=-
// api function to be referenced by the entry

int rs_handle_lustre_records( rsComm_t* _comm, irodsLustreApiInp_t* _inp, irodsLustreApiOut_t** _out ) {

    //using namespace alt_json;

    rodsLog( LOG_NOTICE, "Dynamic API - Lustre API" );

    int status;

    icatSessionStruct *icss;
    status = chlGetRcs( &icss );
    if ( status < 0 || !icss ) {
        return CAT_NOT_OPEN;
    }


    ( *_out ) = ( irodsLustreApiOut_t* )malloc( sizeof( irodsLustreApiOut_t ) );
    ( *_out )->status = 0;

    rodsLog(LOG_NOTICE, "input: [%s]\n", _inp->change_log_json);

    json_map m { json_data { _inp->change_log_json } };

    if (m.find("change_records") == m.end() || m.find("lustre_root_path") == m.end() ||
            m.find("register_path") == m.end() || m.find("resource_id") == m.end()) {
        rodsLog(LOG_ERROR, "JSON received is not valid.  %s", _inp->change_log_json);
        return SYS_API_INPUT_ERR;
    }

    std::string lustre_root_path = read_string_value_from_json_map("lustre_root_path", m);
    lustre_root_path.erase(remove(lustre_root_path.begin(), lustre_root_path.end(), '\"' ), lustre_root_path.end());
    std::string register_path = read_string_value_from_json_map("register_path", m); 
    register_path.erase(remove(register_path.begin(), register_path.end(), '\"' ), register_path.end());
    std::string resource_id_str = read_string_value_from_json_map("resource_id", m);
    resource_id_str.erase(remove(resource_id_str.begin(), resource_id_str.end(), '\"' ), resource_id_str.end());

    jeayeson::array_t arr { json_value { m.find("change_records")->second } };

    rodsLog(LOG_NOTICE, "arr.size = %i\n", arr.size());

    for (size_t i = 0; i < arr.size(); ++i) {
        rodsLog(LOG_NOTICE, "==== %i ====\n", i);
        json_map m2 { json_value { arr[i] } };

        // verify that entry has all required fields
        if (m2.find("event_type") == m2.end() || m2.find("fidstr") == m2.end() ||
                m2.find("lustre_path") == m2.end() || m2.find("object_name") == m2.end() ||
                m2.find("object_type") == m2.end() || m2.find("parent_fidstr") == m2.end() ||
                m2.find("file_size") == m2.end()) {

            // skip entry
            std::stringstream error_msg; 
            error_msg << arr[i];
            rodsLog(LOG_NOTICE, "Skipping entry %s which does not have all of the expected fields.", error_msg.str().c_str());
            continue;
        }


        std::string event_type = read_string_value_from_json_map("event_type", m2);
        event_type.erase(remove(event_type.begin(), event_type.end(), '\"' ), event_type.end());
        std::string fidstr = read_string_value_from_json_map("fidstr", m2);
        fidstr.erase(remove(fidstr.begin(), fidstr.end(), '\"' ), fidstr.end());
        std::string lustre_path = read_string_value_from_json_map("lustre_path", m2);
        lustre_path.erase(remove(lustre_path.begin(), lustre_path.end(), '\"' ), lustre_path.end());
        std::string object_name = read_string_value_from_json_map("object_name", m2);
        object_name.erase(remove(object_name.begin(), object_name.end(), '\"' ), object_name.end());
        std::string object_type = read_string_value_from_json_map("object_type", m2);
        object_type.erase(remove(object_type.begin(), object_type.end(), '\"' ), object_type.end());
        std::string parent_fidstr = read_string_value_from_json_map("parent_fidstr", m2);
        parent_fidstr.erase(remove(parent_fidstr.begin(), parent_fidstr.end(), '\"' ), parent_fidstr.end());
        std::string file_size_str = read_string_value_from_json_map("file_size", m2);
        file_size_str.erase(remove(file_size_str.begin(), file_size_str.end(), '\"' ), file_size_str.end());

        rodsLog(LOG_NOTICE, "event_type: %s\tfidstr: %s\tlustre_path: %s\tobject_name %s\tobject_type: %s\tparent_fidstr %s\tfile_size %s\n",
                event_type.c_str(), fidstr.c_str(), lustre_path.c_str(), object_name.c_str(), object_type.c_str(), parent_fidstr.c_str(), 
                file_size_str.c_str());

        // Handle changes in iRODS.  For efficiency these use lower level routines and do not trigger dynamic PEPs.

        if (event_type == "CREAT") {

            char irods_path[MAX_NAME_LEN];
            if (lustre_path_to_irods_path(lustre_path.c_str(), lustre_root_path.c_str(), register_path.c_str(), irods_path) < 0) {
                rodsLog(LOG_NOTICE, "Skipping entry because lustre_path [%s] is not within lustre_root_path [%s].",
                       lustre_path.c_str(), lustre_root_path.c_str()); 
                continue;

            }

            rodsLong_t file_size;
            rodsLong_t resource_id;

            try {
               file_size = boost::lexical_cast<int64_t>(file_size_str);
               resource_id = boost::lexical_cast<int64_t>(resource_id_str);
            } catch (const boost::bad_lexical_cast &) {
                rodsLog(LOG_NOTICE, "Skipping entry because %s is not a valid size.", file_size_str.c_str());
                continue;
            }

            dataObjInfo_t data_obj_info;
            memset(&data_obj_info, 0, sizeof(data_obj_info));
            strncpy(data_obj_info.objPath, irods_path, MAX_NAME_LEN);
            strncpy(data_obj_info.dataType, "generic", NAME_LEN);
            strncpy(data_obj_info.filePath, lustre_path.c_str(), MAX_NAME_LEN);
            data_obj_info.rescId = resource_id;
            data_obj_info.dataSize = file_size;

            // register object
            status = chlRegDataObj(_comm, &data_obj_info);
            rodsLog(LOG_NOTICE, "Return value from chlRegDataObj = %i", status);
            if (status < 0) {
                rodsLog(LOG_ERROR, "Error registering object %s.  Error is %i", register_path.c_str(), status);
                continue;
            }

            // add fidstr metadata
            keyValPair_t reg_param;
            memset(&reg_param, 0, sizeof(reg_param));
            addKeyVal(&reg_param, "fidstr", fidstr.c_str());
            status = chlAddAVUMetadata(_comm, 0, "-d", irods_path, "fidstr", fidstr.c_str(), "");
            rodsLog(LOG_NOTICE, "Return value from chlAddAVUMetdata = %i", status);
            if (status < 0) {
                rodsLog(LOG_ERROR, "Error adding fidstr metadata to object %s.  Error is %i", register_path.c_str(), status);
                continue;
            }

        } else if (event_type == "MKDIR") {

            // TODO is it better to use the parent_fidstr to find the parent to avoid race conditions?

            char irods_path[MAX_NAME_LEN];
            if (lustre_path_to_irods_path(lustre_path.c_str(), lustre_root_path.c_str(), register_path.c_str(), irods_path) < 0) {
                rodsLog(LOG_NOTICE, "Skipping mkdir on lustre_path [%s] is not within lustre_root_path [%s].",
                       lustre_path.c_str(), lustre_root_path.c_str());
                continue;
            }

            collInfo_t coll_info;
            memset(&coll_info, 0, sizeof(coll_info));
            strncpy(coll_info.collName, irods_path, MAX_NAME_LEN);

            //status = splitPathByKey(irods_path, coll_info.collParentName, MAX_NAME_LEN, coll_info.collName, MAX_NAME_LEN, '/');
            //if (status < 0) {
            //    rodsLog( LOG_ERROR, "Skipping MKDIR on %s because splitPathByKey returned an error, status = %d", irods_path, status );
            //    continue;
            //}

            //rodsLog(LOG_NOTICE, "collParentName=%s collName=%s", coll_info.collParentName,coll_info.collName);

            // register object
            status = chlRegColl(_comm, &coll_info);
            rodsLog(LOG_NOTICE, "Return value from chlRegColl = %i", status);
            if (status < 0) {
                rodsLog(LOG_ERROR, "Error registering collection %s.  Error is %i", register_path.c_str(), status);
                continue;
            } 

            // add fidstr metadata
            keyValPair_t reg_param;
            memset(&reg_param, 0, sizeof(reg_param));
            addKeyVal(&reg_param, "fidstr", fidstr.c_str());
            status = chlAddAVUMetadata(_comm, 0, "-C", irods_path, "fidstr", fidstr.c_str(), "");
            rodsLog(LOG_NOTICE, "Return value from chlAddAVUMetdata = %i", status);
            if (status < 0) {
                rodsLog(LOG_ERROR, "Error adding fidstr metadata to object %s.  Error is %i", register_path.c_str(), status);
                continue;
            }

        } else if (event_type == "OTHER") {

            // read and update the file size

            cllBindVars[0] = file_size_str.c_str();
            cllBindVars[1] = fidstr.c_str(); 
            cllBindVarCount = 2;
            status = cmlExecuteNoAnswerSql(update_data_size_sql, icss);

            if (status != 0) {
                rodsLog(LOG_ERROR, "Error updating data_object_size for data_object %s.  Error is %i", register_path.c_str(), status);
                cmlExecuteNoAnswerSql("rollback", icss);
                continue;
            }

            status =  cmlExecuteNoAnswerSql("commit", icss);

            if (status != 0) {
                rodsLog(LOG_ERROR, "Error committing update to data_object_size for data_object %s.  Error is %i", register_path.c_str(), status);
                continue;
            } 


        } else if (event_type == "RENAME" and object_type == "FILE") {

            // update data_name, data_path, and coll_id
            cllBindVars[0] = object_name.c_str();
            cllBindVars[1] = lustre_path.c_str();
            cllBindVars[2] = parent_fidstr.c_str();
            cllBindVars[3] = fidstr.c_str();
            cllBindVarCount = 4;
            status = cmlExecuteNoAnswerSql(update_data_object_for_rename_sql, icss);

            if (status != 0) {
                rodsLog(LOG_ERROR, "Error updating data object rename for data_object %s.  Error is %i", register_path.c_str(), status);
                cmlExecuteNoAnswerSql("rollback", icss);
                continue;
            }

            status =  cmlExecuteNoAnswerSql("commit", icss);

            if (status != 0) {
                rodsLog(LOG_ERROR, "Error committing update to data object rename for data_object %s.  Error is %i", register_path.c_str(), status);
                continue;
            }

        } else if (event_type == "RENAME" and object_type == "DIR") {
            // TODO

        } else if (event_type == "UNLINK") {
            // TODO

        } else if (event_type == "RMDIR") {
            // TODO

        }




    }

    //rodsLog( LOG_NOTICE, "Received : %s", _inp->change_log_json);
    rodsLog( LOG_NOTICE, "Dynamic API - DONE" );

    return 0;
}

extern "C" {
    // =-=-=-=-=-=-=-
    // factory function to provide instance of the plugin
    irods::api_entry* plugin_factory( const std::string&,     //_inst_name
                                      const std::string& ) { // _context
        // =-=-=-=-=-=-=-
        // create a api def object
        irods::apidef_t def = { 15001,             // api number
                                RODS_API_VERSION, // api version
                                NO_USER_AUTH,     // client auth
                                NO_USER_AUTH,     // proxy auth
                                "IrodsLustreApiInp_PI", 0, // in PI / bs flag
                                "IrodsLustreApiOut", 0, // out PI / bs flag
                                std::function<
                                    int( rsComm_t*,irodsLustreApiInp_t*,irodsLustreApiOut_t**)>(
                                        rs_handle_lustre_records), // operation
								"rs_handle_lustre_records",    // operation name
                                0,  // null clear fcn
                                (funcPtr)CALL_IRODS_LUSTRE_API_INP_OUT
                              };
        // =-=-=-=-=-=-=-
        // create an api object
        irods::api_entry* api = new irods::api_entry( def );

#ifdef RODS_SERVER
        irods::re_serialization::add_operation(
                typeid(irodsLustreApiInp_t*),
                serialize_irodsLustreApiInp_ptr );

        irods::re_serialization::add_operation(
                typeid(irodsLustreApiOut_t**),
                serialize_irodsLustreApiOut_ptr_ptr );
#endif // RODS_SERVER

        // =-=-=-=-=-=-=-
        // assign the pack struct key and value
        api->in_pack_key   = "IrodsLustreApiInp_PI";
        api->in_pack_value = IrodsLustreApiInp_PI;

        api->out_pack_key   = "IrodsLustreApiOut_PI";
        api->out_pack_value = IrodsLustreApiOut_PI;

        return api;

    } // plugin_factory

}; // extern "C"
