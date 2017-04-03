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

int handle_file_add(const char *root_path, struct changelog_rec *rec) {

    // If the file no longer exists just silently return.
    if (!object_exists_in_lustre(root_path, rec)) {
        return 0;
    }

    int rc = 0;
    char lustre_full_path[MAX_NAME_LEN] = "";
    char fidstr[MAX_NAME_LEN] = "";
    char irods_path[MAX_NAME_LEN] = "";

    get_full_path_from_record(root_path, rec, lustre_full_path);

    rc = lustre_path_to_irods_path(lustre_full_path, irods_path);
    if (rc != 0) {
        fprintf(stderr, "Could not translate lustre_path [%s] to irods_path", lustre_full_path);
        return rc;
    }

    rc = register_file(lustre_full_path, irods_path, resource_name);

    if (rc != 0) return rc;

    get_fidstr_from_record(rec, fidstr);
    rc = add_avu(irods_path, "fidstr", fidstr, NULL, false);

    if (rc != 0) return rc;

    sync_modify_time(lustre_full_path, irods_path);

    return 0;

}

int handle_directory_add(const char *root_path, struct changelog_rec *rec) {

    // If the directory no longer exists just silently return
    if (!object_exists_in_lustre(root_path, rec)) {
        return 0;
    }

    int rc = 0;
    char lustre_full_path[MAX_NAME_LEN] = "";
    char fidstr[MAX_NAME_LEN] = "";
    char irods_path[MAX_NAME_LEN] = "";

    get_full_path_from_record(root_path, rec, lustre_full_path);

    rc = lustre_path_to_irods_path(lustre_full_path, irods_path);
    if (rc != 0) {
        fprintf(stderr, "Could not translate lustre_path [%s] to irods_path", lustre_full_path);
        return rc;
    }

    rc = make_collection(irods_path);

    if (rc != 0) return rc;

    get_fidstr_from_record(rec, fidstr);
    return add_avu(irods_path, "fidstr", fidstr, NULL, true);
}

int handle_file_remove(const char *root_path, struct changelog_rec *rec) {

    // If the file no longer exists just silently return
    //if (!object_exists_in_lustre(root_path, rec)) {
    //    return 0;
    //}
   
    char fidstr[MAX_NAME_LEN] = "";
    char irods_path[MAX_NAME_LEN] = "";
 
    get_fidstr_from_record(rec, fidstr);

    // It is possible that the data object was never created.
    if (find_irods_path_with_avu("fidstr", fidstr, NULL, false, irods_path) != 0) {
        return 0;
    }
    return remove_data_object(irods_path);
}

int handle_rename(const char *root_path, struct changelog_rec *rec) {

    // If the file no longer exists just silently return
    if (!object_exists_in_lustre(root_path, rec)) {
        return 0;
    }

    int rc;
    char lustre_full_path[MAX_NAME_LEN] = "";
    char fidstr[MAX_NAME_LEN] = "";
    char new_irods_path[MAX_NAME_LEN] = "";
    char current_irods_path[MAX_NAME_LEN] = "";

    get_full_path_from_record(root_path, rec, lustre_full_path);
    get_fidstr_from_record(rec, fidstr);

    // see if this is a file or directory
    struct stat statbuf;
    stat(lustre_full_path, &statbuf);
    bool is_directory = S_ISDIR(statbuf.st_mode);

    rc = lustre_path_to_irods_path(lustre_full_path, new_irods_path);
    if (rc != 0) {
        fprintf(stderr, "Could not translate lustre_path [%s] to irods_path", lustre_full_path);
        return rc;
    }
  

    // It is possible that the data object was never created
    if (find_irods_path_with_avu("fidstr", fidstr, NULL, is_directory, current_irods_path) != 0) {
        // Just add new data object or collection
        if (is_directory) {
            rc = make_collection(new_irods_path);
        } else {
            rc = register_file(lustre_full_path, new_irods_path, resource_name);
            sync_modify_time(lustre_full_path, new_irods_path);
        }
        if (rc != 0) return rc;

        rc = add_avu(new_irods_path, "fidstr", fidstr, NULL, is_directory);
        return rc;
    }

    // see if the current and new path are the same.  if so just return
    if (strcmp(new_irods_path, current_irods_path) == 0) {
        return 0;
    }

    if (is_directory) {
        rc = rename_collection(current_irods_path, new_irods_path);
    } else {
        rc = rename_data_object(current_irods_path, new_irods_path);
    } 
    if (rc != 0) return rc; 

    if (!is_directory) { 
        rc = update_vault_path_for_data_object(new_irods_path, lustre_full_path);
        sync_modify_time(lustre_full_path, new_irods_path);
    }
   
    return rc;
 
}

int handle_directory_remove(const char *root_path, struct changelog_rec *rec) {

    char fidstr[MAX_NAME_LEN] = "";
    char irods_path[MAX_NAME_LEN] = "";

    get_fidstr_from_record(rec, fidstr);

    // It is possible that the collection was never created.
    if (find_irods_path_with_avu("fidstr", fidstr, NULL, true, irods_path) != 0) {
        return 0;
    }
    return remove_collection(irods_path);
}

int handle_truncate(const char *root_path, struct changelog_rec *rec) {

    char fidstr[MAX_NAME_LEN] = "";
    char irods_path[MAX_NAME_LEN] = "";
    char lustre_full_path[MAX_NAME_LEN] = "";
    int rc = 0;
    struct stat st;
    rodsLong_t file_size;
 
    get_full_path_from_record(root_path, rec, lustre_full_path);
    get_fidstr_from_record(rec, fidstr);
    
    rc = find_irods_path_with_avu("fidstr", fidstr, NULL, false, irods_path);
    if (rc != 0) return rc; 

    if (stat(lustre_full_path, &st) != 0) {
        return errno;
    }

    file_size = st.st_size;

    rc = update_data_object_size(irods_path, file_size);
    if (rc != 0) return rc;

    sync_modify_time(lustre_full_path, irods_path);

    return 0;

}

int handle_update_modify_time(const char *root_path, struct changelog_rec *rec) {

    char fidstr[MAX_NAME_LEN] = "";
    char irods_path[MAX_NAME_LEN] = "";
    char lustre_full_path[MAX_NAME_LEN] = "";
    struct stat file_stat;
    int rc = 0;

    get_full_path_from_record(root_path, rec, lustre_full_path);
    get_fidstr_from_record(rec, fidstr);

    rc = find_irods_path_with_avu("fidstr", fidstr, NULL, false, irods_path);
    if (rc != 0) return rc; 

    return sync_modify_time(lustre_full_path, irods_path);

}

int do_nothing(const char *root_path, struct changelog_rec *rec) {
    return 0;
}

int not_implemented(const char *root_path, struct changelog_rec *rec) {
    printf("OPERATION NOT YET IMPLEMENTED\n");
    return 0;
}

int do_close(const char *root_path, struct changelog_rec *rec) {
    char lustre_full_path[MAX_NAME_LEN] = "";
    get_full_path_from_record(root_path, rec, lustre_full_path);
    printf("CLOSE executed on %s\n", lustre_full_path); 
    return 0;
}

int do_xattr(const char *root_path, struct changelog_rec *rec) {
    char lustre_full_path[MAX_NAME_LEN] = "";
    get_full_path_from_record(root_path, rec, lustre_full_path);
    printf("XATTR executed on %s\n", lustre_full_path);
    return handle_truncate(root_path, rec);
}


int (*lustre_operators[CL_LAST])(const char *, struct changelog_rec *) =
{   &not_implemented,           // CL_MARK
    &handle_file_add,           // CL_CREATE
    &handle_directory_add,      // CL_MKDIR
    &not_implemented,           // CL_HARDLINK
    &not_implemented,           // CL_SOFTLINK
    &do_nothing,                // CL_MKNOD 
    &handle_file_remove,        // CL_UNLINK
    &handle_directory_remove,   // CL_RMDIR
    &handle_rename,             // CL_RENAME
    &do_nothing,                // CL_EXT
    &do_nothing,                // CL_OPEN - not implemented in lustre
    &do_close,                  // CL_CLOSE
    &do_nothing,                // CL_LAYOUT 
    &handle_truncate,           // CL_TRUNC
    &do_nothing,                // CL_SETATTR 
    &do_xattr,                  // CL_XATTR
    &not_implemented,           // CL_HSM
    &handle_update_modify_time, // CL_MTIME
    &do_nothing,                // CL_CTIME - irods does not have a change time
    &do_nothing                 // CL_ATIME - irods does not have an access time
};

int handle_record(const char *root_path, struct changelog_rec *rec) {

    if (rec->cr_type >= CL_LAST) {
        fprintf(stderr, "Invalid cr_type - %i", rec->cr_type);
        return -1;
    }

    
    int rc = (*lustre_operators[rec->cr_type])(root_path, rec);

    if (rc != 0) {
        fprintf(stderr, "Non-zero return code (%i) for operation %s\n", rc, changelog_type2str(rec->cr_type));
    }

    return rc;
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
