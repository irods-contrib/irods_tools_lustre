/*
 * Lustre changlog reader (lcap client) for iRODS.
 */


// TODO Issues:
//   No event for file append.
//   No event for file update time.


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
#include "irods_ops.h"
#include "rodsDef.h"
#include "lustre_change_table.hpp"

#ifndef LPX64
# define LPX64   "%#llx"

static inline bool fid_is_zero(const lustre_fid *fid) {
    return fid->f_seq == 0 && fid->f_oid == 0;
}
#endif

#define SLEEP_TIME 5

const char *mdtname = "lustre01-MDT0000";
const char *lustre_root_path = "/lustre01";
const char *register_path = "/tempZone/lustre";
const char *resource_name = "demoResc";

int sync_modify_time(const char *lustre_full_path, const char *irods_path) {

    struct stat file_stat;
    int rc = 0;

    rc = stat(lustre_full_path, &file_stat);
    if(rc < 0) {
        return rc;
    }

    return update_data_object_modify_time(irods_path, file_stat.st_mtime);

}

// Returns the path in irods for a file in lustre.  
// Precondition:  irods_path is a buffer of size MAX_NAME_LEN
int lustre_path_to_irods_path(const char *lustre_path, char *irods_path) {

    // make sure the file is underneath the lustre_root_path
    if (strncmp(lustre_path, lustre_root_path, strlen(lustre_root_path)) != 0) {
        return -1;
    }

    snprintf(irods_path, MAX_NAME_LEN, "%s%s", register_path, lustre_path + strlen(lustre_root_path));

    return 0;

}

void print_paths(struct changelog_rec *rec, const char *root_path) {

    struct changelog_ext_rename *rnm;

    //char filename[MAX_NAME_LEN];
    //char dirname[MAX_NAME_LEN];
    //char oldfilename[MAX_NAME_LEN];
   
    // Other
    //parent fid - DFID_NOBRACE - PFID(&rec->cr_pfid))
    //filename - DFID_NOBRACE - rec->cr_namelen, changelog_rec_name(rec)

    // Rename
    //rnm = changelog_rec_rename(rec);
    //parent fid old - DFID_NOBRACE - PFID(&rnm->cr_spfid)
    //parent fid new - DFID_NOBRACE - PFID(&rec->cr_pfid)
    //filename old - "%.*s" - (int)changelog_rec_snamelen(rec), changelog_rec_sname(rec)
    //filename new - "%.*s" - rec->cr_namelen, changelog_rec_name(rec)

    char filename[MAX_NAME_LEN];
    char old_filename[MAX_NAME_LEN];
    char parent_fid[MAX_NAME_LEN]; 
    char old_parent_fid[MAX_NAME_LEN];
    char parent_path[MAX_NAME_LEN];
    char old_parent_path[MAX_NAME_LEN];

    long long recno = -1;
    int linkno = 0;


    snprintf(filename, MAX_NAME_LEN, "%.*s", rec->cr_namelen, changelog_rec_name(rec));
    snprintf(parent_fid, MAX_NAME_LEN, DFID_NOBRACE, PFID(&rec->cr_pfid));
    llapi_fid2path(root_path, parent_fid, parent_path, MAX_NAME_LEN, &recno, &linkno);

    printf("Filename: %s\nParentPath: %s\n", filename, parent_path);
    
    switch (rec->cr_type) {
        case CL_RENAME:
            rnm = changelog_rec_rename(rec);
            snprintf(old_filename, MAX_NAME_LEN, "%.*s", (int)changelog_rec_snamelen(rec), changelog_rec_sname(rec));
            snprintf(old_parent_fid, MAX_NAME_LEN, DFID_NOBRACE, PFID(&rnm->cr_spfid));
            llapi_fid2path(root_path, old_parent_fid, old_parent_path, MAX_NAME_LEN, &recno, &linkno);
            printf("Old Filename: %s\nOld ParentPath: %s\n", old_filename, old_parent_path);

            break;
        default:
            //snprintf(fidstr, MAX_NAME_LEN, DFID_NOBRACE, PFID(&rec->cr_tfid));
            break;
    }
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
        fprintf(stderr, "llapi_fid2path in get_full_path_from_record() returned an error.");
        return -1;
    }

    // add root path to lustre_full_path
    char temp[MAX_NAME_LEN] = "";
    snprintf(temp, MAX_NAME_LEN, "%s%s%s", root_path, "/", lustre_full_path);
    strncpy(lustre_full_path, temp, MAX_NAME_LEN);

    return 0;
}

void not_implemented(const char *root_path, const char *lustre_path) {
    printf("OPERATION NOT YET IMPLEMENTED\n");
}


void (*lustre_operators[CL_LAST])(const char *, const char *) =
{   &not_implemented,           // CL_MARK
    &lustre_create,             // CL_CREATE
    &lustre_mkdir,              // CL_MKDIR
    &not_implemented,           // CL_HARDLINK
    &not_implemented,           // CL_SOFTLINK
    &not_implemented,           // CL_MKNOD 
    &lustre_unlink,             // CL_UNLINK
    &lustre_rmdir,              // CL_RMDIR
    &not_implemented,           // CL_RENAME - this is handled as a special case - should never execute
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

void handle_record(const char *root_path, struct changelog_rec *rec) {

    if (rec->cr_type >= CL_LAST) {
        fprintf(stderr, "Invalid cr_type - %i", rec->cr_type);
        return;
    }

    char lustre_full_path[MAX_NAME_LEN] = "";
    char fidstr[MAX_NAME_LEN] = "";

    get_full_path_from_record(root_path, rec, lustre_full_path);
    get_fidstr_from_record(rec, fidstr);

    if (rec->cr_type == CL_RENAME) {
        long long recno = -1;
        int linkno = 0;
        struct changelog_ext_rename *rnm = changelog_rec_rename(rec);
        char old_filename[MAX_NAME_LEN];
        char old_lustre_path[MAX_NAME_LEN];
        char old_parent_fid[MAX_NAME_LEN];
        char old_parent_path[MAX_NAME_LEN];

        snprintf(old_filename, MAX_NAME_LEN, "%.*s", (int)changelog_rec_snamelen(rec), changelog_rec_sname(rec));
        snprintf(old_parent_fid, MAX_NAME_LEN, DFID_NOBRACE, PFID(&rnm->cr_spfid));
        llapi_fid2path(root_path, old_parent_fid, old_parent_path, MAX_NAME_LEN, &recno, &linkno);
        printf("Original old_parent_path = %s\n", old_parent_path);
        if (strlen(old_parent_path) == 0 || old_parent_path[0] != '/') {
            char temp[MAX_NAME_LEN];
            snprintf(temp, MAX_NAME_LEN, "/%s", old_parent_path);
            strncpy(old_parent_path, temp, sizeof(temp));
            //printf("1) Updated old_parent_path = %s\n", old_parent_path);
        }
        if (old_parent_path[strlen(old_parent_path)-1] != '/') {
            char temp[MAX_NAME_LEN];
            snprintf(temp, MAX_NAME_LEN, "%s/", old_parent_path);
            strncpy(old_parent_path, temp, sizeof(temp));
            //printf("2) Updated old_parent_path = %s\n", old_parent_path);
        }
        snprintf(old_lustre_path, MAX_NAME_LEN, "%s%s%s", root_path, old_parent_path, old_filename);

        lustre_rename(fidstr, old_lustre_path, lustre_full_path);
    } else {
        (*lustre_operators[rec->cr_type])(fidstr, lustre_full_path);
    }


}

void get_time_str(time_t time, char *time_str) {

        struct tm * timeinfo;
        timeinfo = localtime(&time);
        strftime(time_str, sizeof(char[20]), "%b %d %H:%M", timeinfo);
}


int main(int ac, char **av)
{
    struct lcap_cl_ctx      *ctx = NULL;
    struct changelog_rec    *rec;
    int                      flags = LCAP_CL_BLOCK;
    int                      c;
    int                      rc;
    char                     clid[64] = {0};

    rc = instantiate_irods_connection(); 
    if (rc < 0) {
        fprintf(stderr, "instantiate_irods_connection failed.  exiting...\n");
        disconnect_irods_connection();
        return 1;
    }

    rc = lcap_changelog_start(&ctx, flags, mdtname, 0LL);
    if (rc < 0) {
        fprintf(stderr, "lcap_changelog_start: %s\n", zmq_strerror(-rc));
        disconnect_irods_connection();
        return 1;
    }

    while (1) {
    sleep(SLEEP_TIME);
    while ((rc = lcap_changelog_recv(ctx, &rec)) == 0) {
        time_t      secs;
        struct tm   ts;

        secs = rec->cr_time >> 30;
        gmtime_r(&secs, &ts);

        printf("%llu %02d%-5s %02d:%02d:%02d.%06d %04d.%02d.%02d 0x%x t="DFID,
               rec->cr_index, rec->cr_type, changelog_type2str(rec->cr_type),
               ts.tm_hour, ts.tm_min, ts.tm_sec,
               (int)(rec->cr_time & ((1 << 30) - 1)),
               ts.tm_year + 1900, ts.tm_mon + 1, ts.tm_mday,
               rec->cr_flags & CLF_FLAGMASK, PFID(&rec->cr_tfid));

        if (rec->cr_flags & CLF_JOBID)
            printf(" j=%s", (const char *)changelog_rec_jobid(rec));

        if (rec->cr_flags & CLF_RENAME) {
            struct changelog_ext_rename *rnm;

            rnm = changelog_rec_rename(rec);
            if (!fid_is_zero(&rnm->cr_sfid))
                printf(" s="DFID" sp="DFID" %.*s", PFID(&rnm->cr_sfid),
                       PFID(&rnm->cr_spfid), (int)changelog_rec_snamelen(rec),
                       changelog_rec_sname(rec));
        }

        if (rec->cr_namelen)
            printf(" p="DFID" %.*s", PFID(&rec->cr_pfid), rec->cr_namelen,
                   changelog_rec_name(rec));

        printf("\n");

        handle_record(lustre_root_path, rec);
        print_paths(rec, lustre_root_path);
        lustre_print_change_table();
 
        rc = lcap_changelog_clear(ctx, mdtname, clid, rec->cr_index);
        if (rc < 0) {
            fprintf(stderr, "lcap_changelog_clear: %s\n", zmq_strerror(-rc));
            disconnect_irods_connection();
            return 1;
        }


        rc = lcap_changelog_free(ctx, &rec);
        if (rc < 0) {
            fprintf(stderr, "lcap_changelog_free: %s\n", zmq_strerror(-rc));
            disconnect_irods_connection();
            return 1;
        }
    }
    }

    if (rc && rc != 1) {
        fprintf(stderr, "lcap_changelog_recv: %s\n", zmq_strerror(-rc));
        disconnect_irods_connection();
        return 1;
    }

    rc = lcap_changelog_fini(ctx);
    if (rc) {
        fprintf(stderr, "lcap_changelog_fini: %s\n", zmq_strerror(-rc));
        disconnect_irods_connection();
        return 1;
    }

    disconnect_irods_connection();
    return 0;
}
