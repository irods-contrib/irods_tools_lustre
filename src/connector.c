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


void get_fidstr_from_record(struct changelog_rec *rec, char *fidstr) {
    sprintf(fidstr, DFID_NOBRACE, PFID(&rec->cr_tfid));
}

int handle_record(const char *root_path, struct changelog_rec *rec, char *fullpath) {

    char fidstr[MAX_NAME_LEN] = "";
    char irods_path[MAX_NAME_LEN] = "";

    switch (rec->cr_type) {

        case CL_CREATE:
            // If we have a create followed by a delete we may not know the full path.  Just return.
            if (strcmp(fullpath, "") == 0)
                return 0;
 
            register_file(fullpath);
            get_fidstr_from_record(rec, fidstr);
            add_avu(fullpath, "fidstr", fidstr, NULL, false);
            break;
        case CL_MKDIR:
            // If we have a create followed by a delete we may not know the full path.  Just return.
            if (strcmp(fullpath, "") == 0)
                return 0;

            make_collection(fullpath);
            get_fidstr_from_record(rec, fidstr);
            add_avu(fullpath, "fidstr", fidstr, NULL, true);
            break;
        case CL_UNLINK:
            get_fidstr_from_record(rec, fidstr);

            // It is possible that the data object was never created.
            if (find_irods_path_with_avu("fidstr", fidstr, NULL, false, irods_path) != 0) {
                return 0;
            }
            remove_data_object(irods_path);
            break;
        case CL_RMDIR:
            get_fidstr_from_record(rec, fidstr);

            // It is possible that the collection was never created.
            if (find_irods_path_with_avu("fidstr", fidstr, NULL, true, irods_path) != 0) {
                return 0;
            }
            remove_collection(irods_path);
            break;
        default:
            printf("OTHER\n");
            break;
    }

    return 0;
}

// Precondition:  fullpath is a buffer of MAX_NAME_LEN length
int get_full_path_from_record(const char *root_path, struct changelog_rec *rec, char *fullpath) {

    char fidstr[MAX_NAME_LEN] = "";
    long long recno = -1;
    int linkno = 0;
    int rc;

    *fullpath = '\0';

    if (rec->cr_type == CL_UNLINK || rec->cr_type == CL_RMDIR) {
	// File or dir no longer exists at this point.  We can't get the path from the fidstr.  
        // Just return. 
        return 0;
    }

    // use fidstr to get path

    get_fidstr_from_record(rec, fidstr);
    rc = llapi_fid2path(root_path, fidstr, fullpath, MAX_NAME_LEN, &recno, &linkno);

    if (rc < 0) {
        // Silently ignore since this may fail if the object has been deleted
        // before this runs.
        return 0;    
    }

    // add root path to fullpath
    char temp[MAX_NAME_LEN] = "";
    snprintf(temp, MAX_NAME_LEN, "%s%s%s", root_path, "/", fullpath);
    strncpy(fullpath, temp, MAX_NAME_LEN);

    return 0;
   
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

        char fullpath[MAX_NAME_LEN];
        get_full_path_from_record(lustre_root_path, rec, fullpath);
        //printf("fullpath %s\n", fullpath);

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

        handle_record(lustre_root_path, rec, fullpath);
 
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
