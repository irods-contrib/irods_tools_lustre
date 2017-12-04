/* This is a wrapper around the Lustre/LCAP interface which can be called directly from C++ source code.
   The standard Lustre headers (called by LCAP) can not be included in C++ due to errors converting 
   enums to ints.
*/

#include "lcap_cpp_wrapper.h"

#include <lcap_client.h>
#include <lcap_config.h>

#include <stdlib.h>
#include <asm/types.h>

int lcap_changelog_wrapper_start(void **pctx, int flags,
                                 const char *mdtname, long long startrec) {
    return lcap_changelog_start((struct lcap_cl_ctx **)pctx, flags, mdtname, startrec);
}


int lcap_changelog_wrapper_fini(void *ctx) {
    return lcap_changelog_fini((struct lcap_cl_ctx *)ctx);
}

int lcap_changelog_wrapper_recv(void *ctx,
                                void **cr) {

    return lcap_changelog_recv((struct lcap_cl_ctx *)ctx, (struct changelog_rec **)cr);
}

int lcap_changelog_wrapper_free(void *ctx,
                                void **cr) {

    return lcap_changelog_free((struct lcap_cl_ctx *)ctx, (struct changelog_rec **)cr);
}

int lcap_changelog_wrapper_clear(void *ctx,
                                 const char *mdtname, const char *id,
                                 long long endrec) {

    return lcap_changelog_clear((struct lcap_cl_ctx *)ctx, mdtname, id, endrec);
}

int get_lcap_cl_block() {
    return LCAP_CL_BLOCK;
}

int llapi_fid2path_wrapper(const char *device, const char *fidstr, char *path,
                      int pathlen, long long *recno, int *linkno) { 

    return llapi_fid2path(device, fidstr, path, pathlen, recno, linkno);
}


void *changelog_rec_wrapper_rename(void *rec) {
    return (void*)changelog_rec_rename((struct changelog_rec *)rec);
}

char *changelog_rec_wrapper_name(void *rec) {
    return changelog_rec_name((struct changelog_rec *)rec);
}    

void *changelog_rec_wrapper_jobid(void *rec) {
    return changelog_rec_jobid((struct changelog_rec *)rec);
}

size_t changelog_rec_wrapper_snamelen(void *rec) {
    return changelog_rec_snamelen((struct changelog_rec *)rec);
}

char *changelog_rec_wrapper_sname(void *rec) {
    return changelog_rec_sname((struct changelog_rec *)rec);
}

const char *changelog_type2str_wrapper(int t) {
    return changelog_type2str(t);
}

__u64 get_f_seq_from_lustre_fid(void *fid) {
    return ((lustre_fid*)(fid))->f_seq;
}

__u32 get_f_oid_from_lustre_fid(void *fid) {
    return ((lustre_fid*)(fid))->f_oid;
}
__u32 get_f_ver_from_lustre_fid(void *fid) {
    return ((lustre_fid*)(fid))->f_ver;
}

__u16 get_cr_namelen_from_changelog_rec(void *rec) {
    return ((struct changelog_rec*)(rec))->cr_namelen;
}

__u16 get_cr_flags_from_changelog_rec(void *rec) {
    return ((struct changelog_rec*)(rec))->cr_flags;
}

__u32 get_cr_type_from_changelog_rec(void *rec) {
    return ((struct changelog_rec*)(rec))->cr_type;
}

__u64 get_cr_index_from_changelog_rec(void *rec) {
    return ((struct changelog_rec*)(rec))->cr_index;
}

__u64 get_cr_prev_from_changelog_rec(void *rec) {
    return ((struct changelog_rec*)(rec))->cr_prev;
}

__u64 get_cr_time_from_changelog_rec(void *rec) {
    return ((struct changelog_rec*)(rec))->cr_time;
}

void *get_cr_tfid_from_changelog_rec(void *rec) {
    lustre_fid *fid = &(((struct changelog_rec*)rec) -> cr_tfid);
    return (void*)fid;
}

void *get_cr_pfid_from_changelog_rec(void *rec) {
    lustre_fid *fid = &(((struct changelog_rec*)rec) -> cr_pfid);
    return (void*)fid;
}

void *get_cr_sfid_from_changelog_ext_rename(void *rnm_rec) {
    lustre_fid *fid = &(((struct changelog_ext_rename*)rnm_rec) -> cr_sfid);
    return (void*)fid;

}

void *get_cr_spfid_from_changelog_ext_rename(void *rnm_rec) {
    lustre_fid *fid = &(((struct changelog_ext_rename*)rnm_rec) -> cr_spfid);
    return (void*)fid;

}

unsigned int get_clf_flagmask() {
    return CLF_FLAGMASK;
}

unsigned int get_clf_rename_mask() {
    return CLF_RENAME;
} 

unsigned int get_clf_jobid_mask() {
    return CLF_JOBID;
}
