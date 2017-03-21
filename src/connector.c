/*
 * Lustre changlog reader (lcap client) for iRODS.
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
#include "irods_ops.h"
#include "rodsDef.h"

#ifndef LPX64
# define LPX64   "%#llx"

static inline bool fid_is_zero(const lustre_fid *fid) {
    return fid->f_seq == 0 && fid->f_oid == 0;
}
#endif

extern const char *mdtname;
extern const char *lustre_root_path;
extern const char *register_path;
extern const char *resource_name;

int (*lustre_operators[CL_LAST])(const char *, struct changelog_rec *);

void get_fidstr_from_record(struct changelog_rec *rec, char *fidstr) {

    struct changelog_ext_rename *rnm;

    switch (rec->cr_type) {

        case CL_RENAME:
            rnm = changelog_rec_rename(rec);
            sprintf(fidstr, DFID_NOBRACE, PFID(&rnm->cr_sfid));
            break;
        default:
            sprintf(fidstr, DFID_NOBRACE, PFID(&rec->cr_tfid));
            break;
    }

}

// Determines if the object (file, dir) from the changelog record still exists in Lustre.
bool object_exists_in_lustre(const char *root_path, struct changelog_rec *rec) {

    char fidstr[MAX_NAME_LEN] = "";
    char fullpath[MAX_NAME_LEN] = "";
    long long recno = -1;
    int linkno = 0;

    int rc;

    // TODO is there a better way than llapi_fid2path to see if file exists
    get_fidstr_from_record(rec, fidstr);

    rc = llapi_fid2path(root_path, fidstr, fullpath, MAX_NAME_LEN, &recno, &linkno);

    if (rc < 0)
        return false;

    return true;
}


// Precondition:  fullpath is a buffer of MAX_NAME_LEN length
int get_full_path_from_record(const char *root_path, struct changelog_rec *rec, char *fullpath) {

    if (!object_exists_in_lustre(root_path, rec)) {
        return -1;        
    }

    char fidstr[MAX_NAME_LEN] = "";
    long long recno = -1;
    int linkno = 0;
    int rc;

    *fullpath = '\0';

    // use fidstr to get path

    get_fidstr_from_record(rec, fidstr);
    rc = llapi_fid2path(root_path, fidstr, fullpath, MAX_NAME_LEN, &recno, &linkno);

    if (rc < 0) {
        fprintf(stderr, "llapi_fid2path in get_full_path_from_record() returned an error.");
        return -1;
    }

    // add root path to fullpath
    char temp[MAX_NAME_LEN] = "";
    snprintf(temp, MAX_NAME_LEN, "%s%s%s", root_path, "/", fullpath);
    strncpy(fullpath, temp, MAX_NAME_LEN);

    return 0;

}

int handle_file_add(const char *root_path, struct changelog_rec *rec) {

    // If the file no longer exists just silently return.
    if (!object_exists_in_lustre(root_path, rec)) {
        return 0;
    }

    int rc = 0;
    char fullpath[MAX_NAME_LEN] = "";
    char fidstr[MAX_NAME_LEN] = "";
    char irods_path[MAX_NAME_LEN] = "";

    get_full_path_from_record(root_path, rec, fullpath);

    rc = register_file(fullpath);

    if (rc != 0) return rc;

    get_fidstr_from_record(rec, fidstr);
    return add_avu(fullpath, "fidstr", fidstr, NULL, false);
   
    
}

int handle_directory_add(const char *root_path, struct changelog_rec *rec) {

    // If the directory no longer exists just silently return
    if (!object_exists_in_lustre(root_path, rec)) {
        return 0;
    }

    int rc = 0;
    char fullpath[MAX_NAME_LEN] = "";
    char fidstr[MAX_NAME_LEN] = "";
    char irods_path[MAX_NAME_LEN] = "";

    get_full_path_from_record(root_path, rec, fullpath);

    rc = make_collection(fullpath);

    if (rc != 0) return rc;

    get_fidstr_from_record(rec, fidstr);
    return add_avu(fullpath, "fidstr", fidstr, NULL, true);
}

int handle_file_remove(const char *root_path, struct changelog_rec *rec) {

    // If the file no longer exists just silently return
    if (!object_exists_in_lustre(root_path, rec)) {
        return 0;
    }
   
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
    char fullpath[MAX_NAME_LEN] = "";
    char fidstr[MAX_NAME_LEN] = "";
    char new_irods_path[MAX_NAME_LEN] = "";
    char current_irods_path[MAX_NAME_LEN] = "";

    get_full_path_from_record(root_path, rec, fullpath);
    get_fidstr_from_record(rec, fidstr);

    // see if this is a file or directory
    struct stat statbuf;
    stat(fullpath, &statbuf);
    bool is_directory = S_ISDIR(statbuf.st_mode);
  

    // It is possible that the data object was never created
    if (find_irods_path_with_avu("fidstr", fidstr, NULL, is_directory, current_irods_path) != 0) {
        // Just add new file 
        if (is_directory) {
            rc = make_collection(fullpath);
        } else {
            rc = register_file(fullpath);
        }
        if (rc != 0) return rc;

        rc = add_avu(fullpath, "fidstr", fidstr, NULL, is_directory);
        return rc;
    }

    // get the new irods path 
    get_irods_path_from_lustre_path(fullpath, new_irods_path);

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
        return update_vault_path_for_data_object(new_irods_path, fullpath); 
    }
   
    return 0;
 
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
    char fullpath[MAX_NAME_LEN] = "";
    int rc = 0;
    struct stat st;
    rodsLong_t file_size;
 
    get_full_path_from_record(root_path, rec, fullpath);
    get_fidstr_from_record(rec, fidstr);
    
    rc = find_irods_path_with_avu("fidstr", fidstr, NULL, false, irods_path);
    if (rc != 0) return rc; 

    if (stat(fullpath, &st) != 0) {
        return errno;
    }

    file_size = st.st_size;

    return update_data_object_size(irods_path, file_size);

}


int not_implemented(const char *root_path, struct changelog_rec *rec) {
    fprintf(stderr, "OPERATION NOT YET IMPLEMENTED");
    return 0;
}

void init_lustre_operators() {
    lustre_operators[CL_MARK] = &not_implemented; 
    lustre_operators[CL_CREATE] = &handle_file_add;
    lustre_operators[CL_MKDIR] = &handle_directory_add;
    lustre_operators[CL_HARDLINK] = &not_implemented;
    lustre_operators[CL_SOFTLINK] = &not_implemented;
    lustre_operators[CL_MKNOD] = &not_implemented;
    lustre_operators[CL_UNLINK] = &handle_file_remove;
    lustre_operators[CL_RMDIR] = &handle_directory_remove;
    lustre_operators[CL_RENAME] = &handle_rename;
    lustre_operators[CL_OPEN] = &not_implemented;
    lustre_operators[CL_CLOSE] = &not_implemented;
    lustre_operators[CL_LAYOUT] = &not_implemented;
    lustre_operators[CL_TRUNC] = &handle_truncate;
    lustre_operators[CL_SETATTR] = &not_implemented;
    lustre_operators[CL_XATTR] = &not_implemented;
    lustre_operators[CL_HSM] = &not_implemented;
    lustre_operators[CL_MTIME] = &not_implemented;
    lustre_operators[CL_CTIME] = &not_implemented;
    lustre_operators[CL_ATIME] = &not_implemented;
}


int handle_record(const char *root_path, struct changelog_rec *rec) {

    if (rec->cr_type >= CL_LAST) {
        fprintf(stderr, "Invalid cr_type - %i", rec->cr_type);
        return -1;
    }

    
    int rc = (*lustre_operators[rec->cr_type])(root_path, rec);
    /*switch (rec->cr_type) {

        case CL_CREATE:
            rc = handle_file_add(root_path, rec);
            break;
        case CL_MKDIR:
            rc = handle_directory_add(root_path, rec);
            break;
        case CL_UNLINK:
            rc = handle_file_remove(root_path, rec);
            break;
        case CL_RMDIR:
            rc = handle_directory_remove(root_path, rec);
            break;
        case CL_RENAME:
            rc = handle_rename(root_path, rec);
            break;
        default:
            printf("OTHER\n");
            break;
    }*/

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

    init_lustre_operators();

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

        // get metadata
        /*struct stat file_stat;
        rc = stat(fullpath, &file_stat);
        if(rc < 0) {
            disconnect_irods_connection();
            return rc;
        }
        printf("Information for %s\n",fullpath);
        printf("---------------------------\n");
        printf("File Size: \t\t%d bytes\n",(int)file_stat.st_size);
        //printf("Number of Links: \t%d\n",file_stat.st_nlink);
        //printf("File inode: \t\t%d\n",file_stat.st_ino);

        printf("Type: \t\t%s\n", S_ISDIR(file_stat.st_mode) ? "directory" : 
                             ( S_ISREG(file_stat.st_mode) ? "regular file" : "other" ) );*/

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
