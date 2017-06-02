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
#include "config.hpp"
#include "lustre_change_table.hpp"

#include "logging.hpp"

#ifndef LPX64
#define LPX64   "%#llx"
static inline bool fid_is_zero(const lustre_fid *fid) {
    return fid->f_seq == 0 && fid->f_oid == 0;
}
#endif

struct lcap_cl_ctx      *ctx = NULL;
int                     flags = LCAP_CL_BLOCK;

// for rename - the overwritten file's fidstr
void get_overwritten_fidstr_from_record(struct changelog_rec *rec, char *fidstr) {
    snprintf(fidstr, MAX_NAME_LEN, DFID_NOBRACE, PFID(&rec->cr_tfid));
}

void get_fidstr_from_record(struct changelog_rec *rec, char *fidstr) {

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

}


// Determines if the object (file, dir) from the changelog record still exists in Lustre.
bool object_exists_in_lustre(const char *root_path, struct changelog_rec *rec) {

    char fidstr[MAX_NAME_LEN] = "";
    char lustre_full_path[MAX_NAME_LEN] = "";
    long long recno = -1;
    int linkno = 0;

    int rc;

    // TODO is there a better way than llapi_fid2path to see if file exists
    get_fidstr_from_record(rec, fidstr);

    rc = llapi_fid2path(root_path, fidstr, lustre_full_path, MAX_NAME_LEN, &recno, &linkno);

    if (rc < 0)
        return false;

    return true;
}


// Precondition:  lustre_full_path is a buffer of MAX_NAME_LEN length
int get_full_path_from_record(const char *root_path, struct changelog_rec *rec, char *lustre_full_path) {

    if (!object_exists_in_lustre(root_path, rec)) {
        return -1;        
    }

    char fidstr[MAX_NAME_LEN] = "";
    long long recno = -1;
    int linkno = 0;
    int rc;

    *lustre_full_path = '\0';

    // use fidstr to get path

    get_fidstr_from_record(rec, fidstr);
    rc = llapi_fid2path(root_path, fidstr, lustre_full_path, MAX_NAME_LEN, &recno, &linkno);

    if (rc < 0) {
        LOG(LOG_ERR, "llapi_fid2path in get_full_path_from_record() returned an error.");
        return -1;
    }

    // add root path to lustre_full_path
    char temp[MAX_NAME_LEN];
    concatenate_paths_with_boost(root_path, lustre_full_path, temp, MAX_NAME_LEN);
    snprintf(lustre_full_path, MAX_NAME_LEN, "%s", temp);

    return 0;
}

void not_implemented(const lustre_irods_connector_cfg_t *config_struct_ptr, const char *fidstr, const char *parent_fidstr, const char *object_path, 
        const char *lustre_path, void *change_map) {
    LOG(LOG_DBG, "OPERATION NOT YET IMPLEMENTED\n");
}


typedef void (*lustre_operation)(const lustre_irods_connector_cfg_t*, const char *, const char *, const char *, const char *, void *);

lustre_operation lustre_operators[CL_LAST] = 
//void (*lustre_operators[CL_LAST])(const lustre_irods_connector_cfg_t*, const char *, const char *, const char *, const char *, void *) =
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

void handle_record(const lustre_irods_connector_cfg_t *config_struct_ptr, struct changelog_rec *rec, void *change_map) {

    if (rec->cr_type >= CL_LAST) {
        LOG(LOG_ERR, "Invalid cr_type - %i", rec->cr_type);
        return;
    }

    char lustre_full_path[MAX_NAME_LEN] = "";
    char fidstr[MAX_NAME_LEN] = "";
    char parent_fidstr[MAX_NAME_LEN] = "";
    char object_name[MAX_NAME_LEN] = "";

    get_fidstr_from_record(rec, fidstr);
    get_full_path_from_record(config_struct_ptr->lustre_root_path, rec, lustre_full_path);
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
        llapi_fid2path(config_struct_ptr->lustre_root_path, old_parent_fid, old_parent_path, MAX_NAME_LEN, &recno, &linkno);
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
        snprintf(old_lustre_path, MAX_NAME_LEN, "%s%s%s", config_struct_ptr->lustre_root_path, old_parent_path, old_filename);

        lustre_rename(config_struct_ptr, fidstr, parent_fidstr, object_name, lustre_full_path, old_lustre_path, change_map);
    } else {
        (*lustre_operators[rec->cr_type])(config_struct_ptr, fidstr, parent_fidstr, object_name, lustre_full_path, change_map);
    }
}

void get_time_str(time_t time, char *time_str) {

        struct tm * timeinfo;
        timeinfo = localtime(&time);
        strftime(time_str, sizeof(char[20]), "%b %d %H:%M", timeinfo);
}

int start_lcap_changelog(const lustre_irods_connector_cfg_t *config_struct_ptr) {
    return lcap_changelog_start(&ctx, flags, config_struct_ptr->mdtname, 0LL);
}

int finish_lcap_changelog() {
    return lcap_changelog_fini(ctx);
}

int poll_change_log_and_process(const lustre_irods_connector_cfg_t *config_struct_ptr, void *change_map) {

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

        if (rec->cr_flags & CLF_JOBID)
            LOG(LOG_INFO, " j=%s", (const char *)changelog_rec_jobid(rec));

        if (rec->cr_flags & CLF_RENAME) {
            struct changelog_ext_rename *rnm;

            rnm = changelog_rec_rename(rec);
            if (!fid_is_zero(&rnm->cr_sfid))
                LOG(LOG_DBG, " s="DFID" sp="DFID" %.*s", PFID(&rnm->cr_sfid),
                       PFID(&rnm->cr_spfid), (int)changelog_rec_snamelen(rec),
                       changelog_rec_sname(rec));
        }

        if (rec->cr_namelen)
            LOG(LOG_DBG, " p="DFID" %.*s", PFID(&rec->cr_pfid), rec->cr_namelen,
                   changelog_rec_name(rec));

        LOG(LOG_INFO, "\n");

        handle_record(config_struct_ptr, rec, change_map);
        //lustre_print_change_table();

        rc = lcap_changelog_clear(ctx, config_struct_ptr->mdtname, clid, rec->cr_index);
        if (rc < 0) {
            LOG(LOG_ERR, "lcap_changelog_clear: %s\n", zmq_strerror(-rc));
            //disconnect_irods_connection();
            return 1;
        }


        rc = lcap_changelog_free(ctx, &rec);
        if (rc < 0) {
            LOG(LOG_ERR, "lcap_changelog_free: %s\n", zmq_strerror(-rc));
            //disconnect_irods_connection();
            return 1;
        }
    }

    return 0;
}
