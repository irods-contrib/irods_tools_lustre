/* This is a wrapper around the Lustre/LCAP interface which can be called directly from C++ source code.
   The standard Lustre headers (called by LCAP) can not be included in C++ due to errors converting 
   ints to enums.
*/


#ifndef __LCAP_CPP_WRAPPER_H
#define __LCAP_CPP_WRAPPER_H

#include <stdlib.h>
#include <asm/types.h>

/** Changelog record types */
enum changelog_rec_type_wrapper {
    CL_MARK_WRAPPER     = 0,
    CL_CREATE_WRAPPER   = 1,  /* namespace */
    CL_MKDIR_WRAPPER    = 2,  /* namespace */
    CL_HARDLINK_WRAPPER = 3,  /* namespace */
    CL_SOFTLINK_WRAPPER = 4,  /* namespace */
    CL_MKNOD_WRAPPER    = 5,  /* namespace */
    CL_UNLINK_WRAPPER   = 6,  /* namespace */
    CL_RMDIR_WRAPPER    = 7,  /* namespace */
    CL_RENAME_WRAPPER   = 8,  /* namespace */
    CL_EXT_WRAPPER      = 9,  /* namespace extended record (2nd half of rename) */
    CL_OPEN_WRAPPER     = 10, /* not currently used */
    CL_CLOSE_WRAPPER    = 11, /* may be written to log only with mtime change */
    CL_LAYOUT_WRAPPER   = 12, /* file layout/striping modified */
    CL_TRUNC_WRAPPER    = 13,
    CL_SETATTR_WRAPPER  = 14,
    CL_XATTR_WRAPPER    = 15,
    CL_HSM_WRAPPER      = 16, /* HSM specific events, see flags */
    CL_MTIME_WRAPPER    = 17, /* Precedence: setattr > mtime > ctime > atime */
    CL_CTIME_WRAPPER    = 18,
    CL_ATIME_WRAPPER    = 19,
    CL_LAST_WRAPPER
};




int lcap_changelog_wrapper_start(void **pctx, int flags,
                                 const char *mdtname, long long startrec); 

int lcap_changelog_wrapper_fini(void *ctx); 

int lcap_changelog_wrapper_recv(void *ctx,
                                void **cr);

int lcap_changelog_wrapper_free(void *ctx,
                                void **cr);

int lcap_changelog_wrapper_clear(void *ctx,
                                 const char *mdtname, const char *id,
                                 long long endrec); 

int get_lcap_cl_block();

int llapi_fid2path_wrapper(const char *device, const char *fidstr, char *path,
                      int pathlen, long long *recno, int *linkno);

void *changelog_rec_wrapper_rename(void *rec); 

char *changelog_rec_wrapper_name(void *rec);

void *changelog_rec_wrapper_jobid(void *rec);

size_t changelog_rec_wrapper_snamelen(void *rec);

char *changelog_rec_wrapper_sname(void *rec);

const char *changelog_type2str_wrapper(int t);

__u64 get_f_seq_from_lustre_fid(void *fid);
__u32 get_f_oid_from_lustre_fid(void *fid);
__u32 get_f_ver_from_lustre_fid(void *fid);

__u16 get_cr_namelen_from_changelog_rec(void *rec);
__u16 get_cr_flags_from_changelog_rec(void *rec);
__u32 get_cr_type_from_changelog_rec(void *rec);
__u64 get_cr_index_from_changelog_rec(void *rec);
__u64 get_cr_prev_from_changelog_rec(void *rec);
__u64 get_cr_time_from_changelog_rec(void *rec);

void *get_cr_tfid_from_changelog_rec(void *rec);
void *get_cr_pfid_from_changelog_rec(void *rec);
void *get_cr_sfid_from_changelog_ext_rename(void *rnm_rec);
void *get_cr_spfid_from_changelog_ext_rename(void *rnm_rec);

unsigned int get_clf_flagmask();
unsigned int get_clf_rename_mask(); 
unsigned int get_clf_jobid_mask(); 

#endif
