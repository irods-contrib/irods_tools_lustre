#ifndef _LUSTRE_IRODS_API_DB_ROUTINES
#define _LUSTRE_IRODS_API_DB_ROUTINES

#include "icatStructs.hpp"

int cmlGetNSeqVals( icatSessionStruct *icss, size_t n, std::vector<rodsLong_t>& sequences );

#if MY_ICAT
void setMysqlIsolationLevelReadCommitted(icatSessionStruct *icss); 
#endif


#endif

