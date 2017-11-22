/*
 * LCAP and Lustre libraries 
 */


#ifdef HAVE_CONFIG_H
#include "lcap_config.h"
#endif

#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <stdbool.h>
#include <string.h>
#include <zmq.h>

#include "lcap_client.h"

#include "rodsDef.h"
//#include "config.hpp"
#include "lustre_change_table.hpp"
#include "lustre_irods_errors.hpp"

#include "logging.hpp"

#ifndef LPX64
#define LPX64   "%#llx"
static inline bool fid_is_zero(const lustre_fid *fid) {
    if (fid == NULL) {
        return true;
    }
    return fid->f_seq == 0 && fid->f_oid == 0;
}
#endif

//struct lcap_cl_ctx      *ctx = NULL;

// for rename - the overwritten file's fidstr
void get_overwritten_fidstr_from_record(struct changelog_rec *rec, char *fidstr) {
    snprintf(fidstr, MAX_NAME_LEN, DFID_NOBRACE, PFID(&rec->cr_tfid));
}

// precondition:  fidstr has MAX_NAME_LEN memory allocated
int get_fidstr_from_record(struct changelog_rec *rec, char *fidstr) {

    if (rec == NULL) {
        LOG(LOG_ERR, "Null rec sent to %s - %d\n", __FUNCTION__, __LINE__);
        return INVALID_OPERAND_ERROR;
    }

    if (fidstr == NULL) {
        LOG(LOG_ERR, "Null fidstr sent to %s - %d\n", __FUNCTION__, __LINE__);
        return INVALID_OPERAND_ERROR;
    }


    struct changelog_ext_rename *rnm;

    switch (rec->cr_type) {

        case CL_RENAME:
            rnm = changelog_rec_rename(rec);
            snprintf(fidstr, MAX_NAME_LEN, DFID_NOBRACE, PFID(&rnm->cr_sfid));
            break;
        default:
            snprintf(fidstr, MAX_NAME_LEN, DFID_NOBRACE, PFID(&rec->cr_tfid));
            break;
    }

    return SUCCESS;

}


// Determines if the object (file, dir) from the changelog record still exists in Lustre.
bool object_exists_in_lustre(const char *root_path, struct changelog_rec *rec) {

    if (root_path == NULL) {
        LOG(LOG_ERR, "Null root_path sent to %s - %d\n", __FUNCTION__, __LINE__);
        return false;
    }

    if (rec == NULL) {
        LOG(LOG_ERR, "Null rec sent to %s - %d\n", __FUNCTION__, __LINE__);
        return INVALID_OPERAND_ERROR;
    }

    char fidstr[MAX_NAME_LEN] = "";
    char lustre_full_path[MAX_NAME_LEN] = "";
    long long recno = -1;
    int linkno = 0;

    int rc;

    // TODO is there a better way than llapi_fid2path to see if file exists
    get_fidstr_from_record(rec, fidstr);

    rc = llapi_fid2path(root_path, fidstr, lustre_full_path, MAX_NAME_LEN, &recno, &linkno);

    if (rc < 0) {
        return false;
    }

    return true;
}


// Precondition:  lustre_full_path is a buffer of MAX_NAME_LEN length
int get_full_path_from_record(const char *root_path, struct changelog_rec *rec, char *lustre_full_path) {

    if (root_path == NULL) {
        LOG(LOG_ERR, "Null root_path sent to %s - %d\n", __FUNCTION__, __LINE__);
        return INVALID_OPERAND_ERROR;
    }

    if (rec == NULL) {
        LOG(LOG_ERR, "Null rec sent to %s - %d\n", __FUNCTION__, __LINE__);
        return INVALID_OPERAND_ERROR;
    }

    if (lustre_full_path == NULL) {
        LOG(LOG_ERR, "Null lustre_full_path sent to %s - %d\n", __FUNCTION__, __LINE__);
        return INVALID_OPERAND_ERROR;
    }

    // this is not necessarily an error.  on a rename or delete the object will
    // no longer exist in lustre
    if (!object_exists_in_lustre(root_path, rec)) {
        return LUSTRE_OBJECT_DNE_ERROR;        
    }

    char fidstr[MAX_NAME_LEN] = "";
    long long recno = -1;
    int linkno = 0;
    int rc;

    *lustre_full_path = '\0';

    // use fidstr to get path

    rc = get_fidstr_from_record(rec, fidstr);
    if (rc < 0) {
        return rc;
    }

    rc = llapi_fid2path(root_path, fidstr, lustre_full_path, MAX_NAME_LEN, &recno, &linkno);

    if (rc < 0) {
        LOG(LOG_ERR, "llapi_fid2path in get_full_path_from_record() returned an error.");
        return LLAPI_FID2PATH_ERROR;
    }

    // add root path to lustre_full_path
    char temp[MAX_NAME_LEN];
    if ((rc = concatenate_paths_with_boost(root_path, lustre_full_path, temp, MAX_NAME_LEN)) != 0) {
        return rc;
    }
    snprintf(lustre_full_path, MAX_NAME_LEN, "%s", temp);

    return SUCCESS;
}

int not_implemented(const char *lustre_root_path, const char *fidstr, const char *parent_fidstr, 
        const char *object_path, const char *lustre_path, void *change_map) {
    LOG(LOG_DBG, "OPERATION NOT IMPLEMENTED\n");
    return SUCCESS;
}


typedef int (*lustre_operation)(const char*, const char *, const char *, const char *, const char *, void *);

lustre_operation lustre_operators[CL_LAST] = 
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

int handle_record(const char *lustre_root_path, struct changelog_rec *rec, void *change_map) {

    if (lustre_root_path == NULL) {
        LOG(LOG_ERR, "Null lustre_root_path sent to %s - %d\n", __FUNCTION__, __LINE__);
        return INVALID_OPERAND_ERROR;
    }

    if (rec == NULL) {
        LOG(LOG_ERR, "Null rec sent to %s - %d\n", __FUNCTION__, __LINE__);
        return INVALID_OPERAND_ERROR;
    }

    if (change_map == NULL) {
        LOG(LOG_ERR, "Null change_map sent to %s - %d\n", __FUNCTION__, __LINE__);
        return INVALID_OPERAND_ERROR;
    }

    if (rec->cr_type >= CL_LAST) {
        LOG(LOG_ERR, "Invalid cr_type - %i", rec->cr_type);
        return INVALID_CR_TYPE_ERROR;
    }

    char lustre_full_path[MAX_NAME_LEN] = "";
    char fidstr[MAX_NAME_LEN] = "";
    char parent_fidstr[MAX_NAME_LEN] = "";
    char object_name[MAX_NAME_LEN] = "";

    get_fidstr_from_record(rec, fidstr);
    int rc = get_full_path_from_record(lustre_root_path, rec, lustre_full_path);
    if (rc != SUCCESS && rc != LUSTRE_OBJECT_DNE_ERROR) {
        return rc;
    }

    snprintf(parent_fidstr, MAX_NAME_LEN, DFID_NOBRACE, PFID(&rec->cr_pfid));
    snprintf(object_name, MAX_NAME_LEN, "%.*s", rec->cr_namelen, changelog_rec_name(rec));


    if (rec->cr_type == CL_RENAME) {

        // remove any entries in table 
        char overwritten_fidstr[MAX_NAME_LEN] = "";
        get_overwritten_fidstr_from_record(rec, overwritten_fidstr); 
        remove_fidstr_from_table(overwritten_fidstr, change_map);

        long long recno = -1;
        int linkno = 0;
        struct changelog_ext_rename *rnm = changelog_rec_rename(rec);
        char old_filename[MAX_NAME_LEN];
        char old_lustre_path[MAX_NAME_LEN];
        char old_parent_fid[MAX_NAME_LEN];
        char old_parent_path[MAX_NAME_LEN];

        snprintf(old_filename, MAX_NAME_LEN, "%.*s", (int)changelog_rec_snamelen(rec), changelog_rec_sname(rec));
        snprintf(old_parent_fid, MAX_NAME_LEN, DFID_NOBRACE, PFID(&rnm->cr_spfid));
        llapi_fid2path(lustre_root_path, old_parent_fid, old_parent_path, MAX_NAME_LEN, &recno, &linkno);
        if (strlen(old_parent_path) == 0 || old_parent_path[0] != '/') {
            char temp[MAX_NAME_LEN];
            snprintf(temp, MAX_NAME_LEN, "/%s", old_parent_path);
            snprintf(old_parent_path, MAX_NAME_LEN, "%s", temp);
            //printf("1) Updated old_parent_path = %s\n", old_parent_path);
        }
        if (old_parent_path[strlen(old_parent_path)-1] != '/') {
            char temp[MAX_NAME_LEN];
            snprintf(temp, MAX_NAME_LEN, "%s/", old_parent_path);
            snprintf(old_parent_path, MAX_NAME_LEN, "%s", temp);
        }
        snprintf(old_lustre_path, MAX_NAME_LEN, "%s%s%s", lustre_root_path, old_parent_path, old_filename);

        return lustre_rename(lustre_root_path, fidstr, parent_fidstr, object_name, lustre_full_path, old_lustre_path, change_map);
    } else {
        return (*lustre_operators[rec->cr_type])(lustre_root_path, fidstr, parent_fidstr, object_name, lustre_full_path, change_map);
    }
}

/*int get_time_str(time_t time, char *time_str) {
 
    struct tm * timeinfo;
    timeinfo = localtime(&time);
    strftime(time_str, sizeof(char[20]), "%b %d %H:%M", timeinfo);
}*/

int start_lcap_changelog(const char *mdtname, struct lcap_cl_ctx **ctx) {

    if (mdtname == NULL) {
        LOG(LOG_ERR, "Null mdtname sent to %s - %d\n", __FUNCTION__, __LINE__);
        return INVALID_OPERAND_ERROR;
    }

    return lcap_changelog_start(ctx, LCAP_CL_BLOCK, mdtname, 0LL);
}

int finish_lcap_changelog(struct lcap_cl_ctx *ctx) {
    return lcap_changelog_fini(ctx);
}

int poll_change_log_and_process(const char *mdtname, const char *lustre_root_path, void *change_map, struct lcap_cl_ctx *ctx) {


    if (mdtname == NULL) {
        LOG(LOG_ERR, "Null mdtname sent to %s - %d\n", __FUNCTION__, __LINE__);
        return INVALID_OPERAND_ERROR;
    }
    if (lustre_root_path == NULL) {
        LOG(LOG_ERR, "Null lustre_root_path sent to %s - %d\n", __FUNCTION__, __LINE__);
        return INVALID_OPERAND_ERROR;
    }


    if (change_map == NULL) {
        LOG(LOG_ERR, "Null change_map sent to %s - %d\n", __FUNCTION__, __LINE__);
        return INVALID_OPERAND_ERROR;
    }


    int                    rc;
    char                   clid[64] = {0};
    struct changelog_rec   *rec;

    while ((rc = lcap_changelog_recv(ctx, &rec)) == 0) {
        time_t      secs;
        struct tm   ts;

        secs = rec->cr_time >> 30;
        gmtime_r(&secs, &ts);

        LOG(LOG_INFO, "%llu %02d%-5s %02d:%02d:%02d.%06d %04d.%02d.%02d 0x%x t="DFID,
               rec->cr_index, rec->cr_type, changelog_type2str(rec->cr_type),
               ts.tm_hour, ts.tm_min, ts.tm_sec,
               (int)(rec->cr_time & ((1 << 30) - 1)),
               ts.tm_year + 1900, ts.tm_mon + 1, ts.tm_mday,
               rec->cr_flags & CLF_FLAGMASK, PFID(&rec->cr_tfid));

        if (rec->cr_flags & CLF_JOBID) {
            LOG(LOG_INFO, " j=%s", (const char *)changelog_rec_jobid(rec));
        }

        if (rec->cr_flags & CLF_RENAME) { 
            struct changelog_ext_rename *rnm;

            rnm = changelog_rec_rename(rec);
            if (!fid_is_zero(&rnm->cr_sfid)) {
                LOG(LOG_DBG, " s="DFID" sp="DFID" %.*s", PFID(&rnm->cr_sfid),
                       PFID(&rnm->cr_spfid), (int)changelog_rec_snamelen(rec),
                       changelog_rec_sname(rec));
            }
        }

        if (rec->cr_namelen) {
            LOG(LOG_DBG, " p="DFID" %.*s", PFID(&rec->cr_pfid), rec->cr_namelen,
                   changelog_rec_name(rec));
        }

        LOG(LOG_INFO, "\n");

        if ((rc = handle_record(lustre_root_path, rec, change_map)) < 0) {
            LOG(LOG_ERR, "handle record failed for %s "DFID" rc = %i\n", changelog_type2str(rec->cr_type), PFID(&rec->cr_tfid), rc);
        }

        rc = lcap_changelog_clear(ctx, mdtname, clid, rec->cr_index);
        if (rc < 0) {
            LOG(LOG_ERR, "lcap_changelog_clear: %s\n", zmq_strerror(-rc));
            return rc;
        }

        rc = lcap_changelog_free(ctx, &rec);
        if (rc < 0) {
            LOG(LOG_ERR, "lcap_changelog_free: %s\n", zmq_strerror(-rc));
            return rc;
        }
    }

    return SUCCESS;
}

