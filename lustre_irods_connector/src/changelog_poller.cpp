/*
 * Contains functions that deal with LCAP and Lustre interfaces.  All LCAP and Lustre interfaces
 * go through the lcap_cpp_wrapper.h functions.
 */


// standard headers 
#include <string>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <stdbool.h>
#include <string.h>
#include <zmq.h>

// boost headers 
#include <boost/format.hpp>

// irods headers 
#include "rodsDef.h"

// lustre irods connector headers
#include "lustre_change_table.hpp"
#include "lustre_irods_errors.hpp"
#include "logging.hpp"
#include "changelog_poller.hpp"

extern "C" {
  #include "lcap_cpp_wrapper.h"
}


std::string concatenate_paths_with_boost(const std::string& p1, const std::string& p2) {

    boost::filesystem::path path_obj_1{p1};
    boost::filesystem::path path_obj_2{p2};
    boost::filesystem::path path_result = path_obj_1/path_obj_2;
    return path_result.string();
} 

static inline bool fid_is_zero(lustre_fid_ptr fid) {
    if (fid == NULL) {
        return true;
    }
    return get_f_seq_from_lustre_fid(fid) == 0 && get_f_oid_from_lustre_fid(fid) == 0;
}

static boost::format fid_format_obj("%#llx:0x%x:0x%x");

std::string convert_to_fidstr(lustre_fid_ptr fid) {
    return str(fid_format_obj % get_f_seq_from_lustre_fid(fid) % 
            get_f_oid_from_lustre_fid(fid) % 
            get_f_ver_from_lustre_fid(fid));
}

// for rename - the overwritten file's fidstr
std::string get_overwritten_fidstr_from_record(changelog_rec_ptr rec) {
    return convert_to_fidstr(get_cr_tfid_from_changelog_rec(rec)); 
}

int get_fidstr_from_record(changelog_rec_ptr rec, std::string& fidstr) {

    if (rec == NULL) {
        LOG(LOG_ERR, "Null rec sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    changelog_ext_rename_ptr rnm;


    if (get_cr_type_from_changelog_rec(rec) == get_cl_rename()) {
        rnm = changelog_rec_wrapper_rename(rec);
        fidstr = convert_to_fidstr(get_cr_sfid_from_changelog_ext_rename(rnm));
    } else {
        fidstr = convert_to_fidstr(get_cr_tfid_from_changelog_rec(rec));
    }

    return lustre_irods::SUCCESS;

}


// Determines if the object (file, dir) from the changelog record still exists in Lustre.
bool object_exists_in_lustre(const std::string& root_path, changelog_rec_ptr rec) {

    if (rec == NULL) {
        LOG(LOG_ERR, "Null rec sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    std::string fidstr;
    char lustre_full_path[MAX_NAME_LEN] = "";
    long long recno = -1;
    int linkno = 0;

    int rc;

    // TODO is there a better way than llapi_fid2path to see if file exists
    get_fidstr_from_record(rec, fidstr);

    rc = llapi_fid2path_wrapper(root_path.c_str(), fidstr.c_str(), lustre_full_path, MAX_NAME_LEN, &recno, &linkno);

    if (rc < 0) {
        return false;
    }

    return true;
}


int get_full_path_from_record(const std::string& root_path, changelog_rec_ptr rec, std::string& lustre_full_path) {

    if (rec == NULL) {
        LOG(LOG_ERR, "Null rec sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    // this is not necessarily an error.  on a rename or delete the object will
    // no longer exist in lustre
    if (!object_exists_in_lustre(root_path, rec)) {
        return lustre_irods::LUSTRE_OBJECT_DNE_ERROR;        
    }

    std::string fidstr;
    long long recno = -1;
    int linkno = 0;
    int rc;

    char lustre_full_path_cstr[MAX_NAME_LEN] = {};

    // use fidstr to get path

    rc = get_fidstr_from_record(rec, fidstr);
    if (rc < 0) {
        return rc;
    }

    rc = llapi_fid2path_wrapper(root_path.c_str(), fidstr.c_str(), lustre_full_path_cstr, MAX_NAME_LEN, &recno, &linkno);

    if (rc < 0) {
        LOG(LOG_ERR, "llapi_fid2path in get_full_path_from_record() returned an error.");
        return lustre_irods::LLAPI_FID2PATH_ERROR;
    }

    // add root path to lustre_full_path
    lustre_full_path = concatenate_paths_with_boost(root_path, lustre_full_path_cstr);

    return lustre_irods::SUCCESS;
}

int not_implemented(const std::string& lustre_root_path, const std::string& fidstr, const std::string& parent_fidstr, 
        const std::string& object_path, const std::string& lustre_path, change_map_t& change_map) {
    LOG(LOG_DBG, "OPERATION NOT IMPLEMENTED\n");
    return lustre_irods::SUCCESS;
}


//typedef int (*lustre_operation)(const std::string&, const std::string&, const std::string&, const std::string&, const std::string&, change_map_t&);

//std::vector<lustre_operation> lustre_operators = 
std::vector<std::function<int(const std::string&, const std::string&, const std::string&, const std::string&, const std::string&, change_map_t&)> > lustre_operators = 
{   &not_implemented,           // CL_MARK
    &lustre_create,             // CL_CREATE
    &lustre_mkdir,              // CL_MKDIR
    &not_implemented,           // CL_HARDLINK
    &not_implemented,           // CL_SOFTLINK
    &not_implemented,           // CL_MKNOD 
    &lustre_unlink,             // CL_UNLINK
    &lustre_rmdir,              // CL_RMDIR
    &not_implemented,           // CL_RENAME - handled as a special case, this should not be executed
    &not_implemented,           // CL_EXT
    &not_implemented,           // CL_OPEN - not implemented in lustre
    &lustre_close,              // CL_CLOSE
    &not_implemented,           // CL_LAYOUT 
    &lustre_trunc,              // CL_TRUNC
    &not_implemented,           // CL_SETATTR 
    &lustre_trunc,              // CL_XATTR
    &not_implemented,           // CL_HSM
    &lustre_mtime,              // CL_MTIME
    &not_implemented,           // CL_CTIME - irods does not have a change time
    &not_implemented            // CL_ATIME - irods does not have an access time
};

int handle_record(const std::string& lustre_root_path, changelog_rec_ptr rec, change_map_t& change_map) {

    if (rec == NULL) {
        LOG(LOG_ERR, "Null rec sent to %s - %d\n", __FUNCTION__, __LINE__);
        return lustre_irods::INVALID_OPERAND_ERROR;
    }

    if (get_cr_type_from_changelog_rec(rec) >= get_cl_last()) {
        LOG(LOG_ERR, "Invalid cr_type - %u\n", get_cr_type_from_changelog_rec(rec));
        return lustre_irods::INVALID_CR_TYPE_ERROR;
    }

    std::string lustre_full_path;
    std::string fidstr;

    get_fidstr_from_record(rec, fidstr);
    int rc = get_full_path_from_record(lustre_root_path, rec, lustre_full_path);
    if (rc != lustre_irods::SUCCESS && rc != lustre_irods::LUSTRE_OBJECT_DNE_ERROR) {
        return rc;
    }

    std::string parent_fidstr = convert_to_fidstr(get_cr_pfid_from_changelog_rec(rec));

    std::string object_name = changelog_rec_wrapper_name(rec);

    if (get_cr_type_from_changelog_rec(rec) == get_cl_rename()) {

        // remove any entries in table 
        std::string overwritten_fidstr = get_overwritten_fidstr_from_record(rec); 
        remove_fidstr_from_table(overwritten_fidstr, change_map);

        long long recno = -1;
        int linkno = 0;
        changelog_ext_rename_ptr rnm = changelog_rec_wrapper_rename(rec);
        std::string old_filename;
        std::string old_lustre_path;
        std::string old_parent_fid;
        std::string old_parent_path;

        old_filename = std::string(changelog_rec_wrapper_sname(rec)).substr(0, (int)changelog_rec_wrapper_snamelen(rec));

        old_parent_fid = convert_to_fidstr(get_cr_spfid_from_changelog_ext_rename(rnm));

        char old_parent_path_cstr[MAX_NAME_LEN] = {};
        rc = llapi_fid2path_wrapper(lustre_root_path.c_str(), old_parent_fid.c_str(), old_parent_path_cstr, MAX_NAME_LEN, &recno, &linkno);
        if (rc < 0) {
            LOG(LOG_ERR, "llapi_fid2path in %s returned an error.", __FUNCTION__);
            return lustre_irods::LLAPI_FID2PATH_ERROR;
        } 

        old_parent_path = old_parent_path_cstr;

        // add a leading '/' if necessary
        if (old_parent_path.length() == 0 || old_parent_path[0] != '/') {
            old_parent_path = "/" + old_parent_path;
        }
        // add a trailing '/' if necessar
        if (old_parent_path[old_parent_path.length()-1] != '/') {
             old_parent_path += "/";
        }

        old_lustre_path = lustre_root_path + old_parent_path + old_filename;

        return lustre_rename(lustre_root_path, fidstr, parent_fidstr, object_name, lustre_full_path, old_lustre_path, change_map);
    } else {
        //return (*lustre_operators[get_cr_type_from_changelog_rec(rec)])(lustre_root_path, fidstr, parent_fidstr, object_name, lustre_full_path, change_map);
        return lustre_operators[get_cr_type_from_changelog_rec(rec)](lustre_root_path, fidstr, parent_fidstr, object_name, lustre_full_path, change_map);
    }
}

int start_lcap_changelog(const std::string& mdtname, lcap_cl_ctx_ptr *ctx) {
    // TODO can I start at something other than 0LL in case there are multiple listeners so the changelog and the changelog doesn't get cleared?
    return lcap_changelog_wrapper_start(ctx, get_lcap_cl_block(), mdtname.c_str(), 0LL);
}

int finish_lcap_changelog(lcap_cl_ctx_ptr ctx) {
    return lcap_changelog_wrapper_fini(ctx);
}

// Poll the change log and process.
// Arguments:
//   mdtname - the name of the mdt
//   lustre_root_path - the root path of the lustre mount point
//   change_map - the change map
//   ctx - the lcap context (lcap_cl_ctx) 
int poll_change_log_and_process(const std::string& mdtname, const std::string& lustre_root_path, change_map_t& change_map, lcap_cl_ctx_ptr ctx) {

    int                    rc;
    char                   clid[64] = {0};

    // the changelog_rec 
    changelog_rec_ptr rec;

    while ((rc = lcap_changelog_wrapper_recv(ctx, &rec)) == 0) {
        time_t      secs;
        struct tm   ts;

        //secs = get_cr_time_from_changelog_rec(rec) >> 30;
        secs = get_cr_time_from_changelog_rec(rec) >> 30;
        gmtime_r(&secs, &ts);

        lustre_fid_ptr cr_tfid_ptr = get_cr_tfid_from_changelog_rec(rec);

        LOG(LOG_INFO, "%llu %02d%-5s %02d:%02d:%02d.%06d %04d.%02d.%02d 0x%x t=%#llx:0x%x:0x%x",
               get_cr_index_from_changelog_rec(rec), get_cr_type_from_changelog_rec(rec), 
               changelog_type2str_wrapper(get_cr_type_from_changelog_rec(rec)), 
               ts.tm_hour, ts.tm_min, ts.tm_sec,
               (int)(get_cr_time_from_changelog_rec(rec) & ((1 << 30) - 1)),
               ts.tm_year + 1900, ts.tm_mon + 1, ts.tm_mday,
               get_cr_flags_from_changelog_rec(rec) & get_clf_flagmask(), 
               get_f_seq_from_lustre_fid(cr_tfid_ptr),
               get_f_oid_from_lustre_fid(cr_tfid_ptr),
               get_f_ver_from_lustre_fid(cr_tfid_ptr));

        if (get_cr_flags_from_changelog_rec(rec) & get_clf_jobid_mask()) {
            LOG(LOG_INFO, " j=%s", (const char *)changelog_rec_wrapper_jobid(rec));
        }

        if (get_cr_flags_from_changelog_rec(rec) & get_clf_rename_mask()) { 
            changelog_ext_rename_ptr rnm;

            rnm = changelog_rec_wrapper_rename(rec);
            if (!fid_is_zero(get_cr_sfid_from_changelog_ext_rename(rnm))) {
                lustre_fid_ptr cr_sfid_ptr = get_cr_sfid_from_changelog_ext_rename(rnm);
                lustre_fid_ptr cr_spfid_ptr = get_cr_spfid_from_changelog_ext_rename(rnm);
                LOG(LOG_DBG, " s=%#llx:0x%x:0x%x sp=%#llx:0x%x:0x%x %.*s", 
                       get_f_seq_from_lustre_fid(cr_sfid_ptr),
                       get_f_oid_from_lustre_fid(cr_sfid_ptr),
                       get_f_ver_from_lustre_fid(cr_sfid_ptr),
                       get_f_seq_from_lustre_fid(cr_spfid_ptr),
                       get_f_oid_from_lustre_fid(cr_spfid_ptr),
                       get_f_ver_from_lustre_fid(cr_spfid_ptr),
                       (int)changelog_rec_wrapper_snamelen(rec),
                       changelog_rec_wrapper_sname(rec));
            }
        }

        // if rename
        if (get_cr_namelen_from_changelog_rec(rec)) {
            lustre_fid_ptr cr_pfid_ptr = get_cr_pfid_from_changelog_rec(rec);
            LOG(LOG_DBG, " p=%#llx:0x%x:0x%x %.*s", 
                    get_f_seq_from_lustre_fid(cr_pfid_ptr),
                    get_f_oid_from_lustre_fid(cr_pfid_ptr),
                    get_f_ver_from_lustre_fid(cr_pfid_ptr),
                    get_cr_namelen_from_changelog_rec(rec),
                    changelog_rec_wrapper_name(rec));
        }

        LOG(LOG_INFO, "\n");

        if ((rc = handle_record(lustre_root_path, rec, change_map)) < 0) {
            lustre_fid_ptr cr_tfid_ptr = get_cr_tfid_from_changelog_rec(rec);
            LOG(LOG_ERR, "handle record failed for %s %#llx:0x%x:0x%x rc = %i\n", 
                    changelog_type2str_wrapper(get_cr_type_from_changelog_rec(rec)), 
                    get_f_seq_from_lustre_fid(cr_tfid_ptr),
                    get_f_oid_from_lustre_fid(cr_tfid_ptr),
                    get_f_ver_from_lustre_fid(cr_tfid_ptr),
                    rc);
        }

        rc = lcap_changelog_wrapper_clear(ctx, mdtname.c_str(), clid, get_cr_index_from_changelog_rec(rec));
        if (rc < 0) {
            LOG(LOG_ERR, "lcap_changelog_clear: %s\n", zmq_strerror(-rc));
            return rc;
        }

        rc = lcap_changelog_wrapper_free(ctx, &rec);
        if (rc < 0) {
            LOG(LOG_ERR, "lcap_changelog_free: %s\n", zmq_strerror(-rc));
            return rc;
        }
    }

    return lustre_irods::SUCCESS;
}

