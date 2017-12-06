#ifndef IRODS_LUSTRE_INOUT_STRUCTS
#define IRODS_LUSTRE_INOUT_STRUCTS

typedef struct {
    int buflen;
    unsigned char *buf;
} irodsLustreApiInp_t;
#define IrodsLustreApiInp_PI "int buflen; bin *buf(buflen);"

typedef struct {
    int status;
} irodsLustreApiOut_t;
#define IrodsLustreApiOut_PI "int status;"

#endif


