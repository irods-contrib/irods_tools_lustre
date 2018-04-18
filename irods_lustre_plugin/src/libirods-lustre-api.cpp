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

#include "boost/lexical_cast.hpp"
#include "boost/filesystem.hpp"

#include "database_routines.hpp"

// =-=-=-=-=-=-=-
// stl includes
#include <sstream>
#include <string>
#include <iostream>
#include <vector>

// json header
//#include <jeayeson/jeayeson.hpp>

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
            _out["buf"] = boost::lexical_cast<std::string>(tmp->buf);
        }
        else {
            _out["buf"] = "";
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

    rodsLog( LOG_NOTICE, "Dynamic API - Lustre API" );

    // read the serialized input
    const kj::ArrayPtr<const capnp::word> array_ptr{ reinterpret_cast<const capnp::word*>(&(*(_inp->buf))), 
        reinterpret_cast<const capnp::word*>(&(*(_inp->buf + _inp->buflen)))};
    capnp::FlatArrayMessageReader message(array_ptr);

    ChangeMap::Reader changeMap = message.getRoot<ChangeMap>();
    std::string irods_api_update_type(changeMap.getIrodsApiUpdateType().cStr()); 
    bool direct_db_modification_requested = (irods_api_update_type == "direct");

    int status;
    icatSessionStruct *icss = nullptr;

    // Bulk request must be performed on an iCAT server if doing direct DB access.  If this is not the iCAT, 
    // forward this request to it.
    if (direct_db_modification_requested) {

        rodsServerHost_t *rodsServerHost;
        status = getAndConnRcatHost(_comm, MASTER_RCAT, (const char*)nullptr, &rodsServerHost);
        if ( status < 0 ) {
            rodsLog(LOG_ERROR, "Error:  getAndConnRcatHost returned %d", status);
            return status;
        }

        if ( rodsServerHost->localFlag != LOCAL_HOST ) {
            rodsLog(LOG_NOTICE, "Bulk request received by catalog consumer.  Forwarding request to catalog provider.");
            status = procApiRequest(rodsServerHost->conn, 15001, _inp, nullptr, (void**)_out, nullptr);
            return status;
        }

        std::string svc_role;
        irods::error ret = get_catalog_service_role(svc_role);
        if(!ret.ok()) {
            irods::log(PASS(ret));
            return ret.code();
        }

        if (irods::CFG_SERVICE_ROLE_PROVIDER != svc_role) {
            rodsLog(LOG_ERROR, "Error:  Attempting bulk Lustre operations on a catalog consumer.  Must connect to catalog provider.");
            return CAT_NOT_OPEN;
        }

        status = chlGetRcs( &icss );
        if ( status < 0 || !icss ) {
            return CAT_NOT_OPEN;
        }
    }


    // setup the output struct
    ( *_out ) = ( irodsLustreApiOut_t* )malloc( sizeof( irodsLustreApiOut_t ) );
    ( *_out )->status = 0;

    rodsLong_t user_id;

    // if we are using direct db access, we need to get the user_name from the user_id
    if (direct_db_modification_requested) {
        status = get_user_id(_comm, icss, user_id, direct_db_modification_requested);
        if (status != 0) {
           rodsLog(LOG_ERROR, "Error getting user_id for user %s.  Error is %i", _comm->clientUser.userName, status);
           return SYS_USER_RETRIEVE_ERR;
        }
    }

    std::string lustre_root_path(changeMap.getLustreRootPath().cStr()); 
    std::string register_path(changeMap.getRegisterPath().cStr()); 
    int64_t resource_id = changeMap.getResourceId();
    std::string resource_name(changeMap.getResourceName().cStr());

    for (ChangeDescriptor::Reader entry : changeMap.getEntries()) {

        const ChangeDescriptor::EventTypeEnum event_type = entry.getEventType();
        std::string fidstr(entry.getFidstr().cStr());
        std::string lustre_path(entry.getLustrePath().cStr());
        std::string object_name(entry.getObjectName().cStr());
        const ChangeDescriptor::ObjectTypeEnum object_type = entry.getObjectType();
        std::string parent_fidstr(entry.getParentFidstr().cStr());
        int64_t file_size = entry.getFileSize();

        // Handle changes in iRODS

        if (event_type == ChangeDescriptor::EventTypeEnum::CREATE) {
            handle_create(lustre_root_path, register_path, resource_id, resource_name,
                    fidstr, lustre_path, object_name, object_type, parent_fidstr, file_size,
                    _comm, icss, user_id, direct_db_modification_requested);
        } else if (event_type == ChangeDescriptor::EventTypeEnum::MKDIR) {
            handle_mkdir(lustre_root_path, register_path, resource_id, resource_name,
                    fidstr, lustre_path, object_name, object_type, parent_fidstr, file_size,
                    _comm, icss, user_id, direct_db_modification_requested);
        } else if (event_type == ChangeDescriptor::EventTypeEnum::OTHER) {
            handle_other(lustre_root_path, register_path, resource_id, resource_name,
                    fidstr, lustre_path, object_name, object_type, parent_fidstr, file_size,
                    _comm, icss, user_id, direct_db_modification_requested);
        } else if (event_type == ChangeDescriptor::EventTypeEnum::RENAME and object_type == ChangeDescriptor::ObjectTypeEnum::FILE) {
            handle_rename_file(lustre_root_path, register_path, resource_id, resource_name,
                    fidstr, lustre_path, object_name, object_type, parent_fidstr, file_size,
                    _comm, icss, user_id, direct_db_modification_requested);
        } else if (event_type == ChangeDescriptor::EventTypeEnum::RENAME and object_type == ChangeDescriptor::ObjectTypeEnum::DIR) {
            handle_rename_dir(lustre_root_path, register_path, resource_id, resource_name,
                    fidstr, lustre_path, object_name, object_type, parent_fidstr, file_size,
                    _comm, icss, user_id, direct_db_modification_requested);
        } else if (event_type == ChangeDescriptor::EventTypeEnum::UNLINK) {
            handle_unlink(lustre_root_path, register_path, resource_id, resource_name,
                    fidstr, lustre_path, object_name, object_type, parent_fidstr, file_size,
                    _comm, icss, user_id, direct_db_modification_requested);
        } else if (event_type == ChangeDescriptor::EventTypeEnum::RMDIR) {
            handle_rmdir(lustre_root_path, register_path, resource_id, resource_name,
                    fidstr, lustre_path, object_name, object_type, parent_fidstr, file_size,
                    _comm, icss, user_id, direct_db_modification_requested);
        }

    }

    //if (direct_db_modification_requested) {
    //    status = cmlClose(icss);
    //    rodsLog(LOG_NOTICE, "cmlClose status = %d", status);
    //}

    rodsLog(LOG_NOTICE, "Dynamic Lustre API - DONE" );

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
