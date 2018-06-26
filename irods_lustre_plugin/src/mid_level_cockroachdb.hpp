/*** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***/
/*******************************************************************************

   This file contains all the  externs of globals used by ICAT

*******************************************************************************/

#ifndef ICAT_MIDLEVEL_ROUTINES_HPP
#define ICAT_MIDLEVEL_ROUTINES_HPP

#include "rodsType.h"
#include "icatStructs.hpp"
#include "low_level_cockroachdb.hpp"

#include <vector>
#include <string>

int cmlOpen( icatSessionStruct *icss, const std::string &host, int port, const std::string &dbname );

int cmlClose( icatSessionStruct *icss );

int cmlExecuteNoAnswerSql( const char *sql,
                           const icatSessionStruct *icss );
int cmlExecuteNoAnswerSqlBV( const char *sql,
                           const std::vector<std::string> &bindVars,
                           const icatSessionStruct *icss );
int cmlGetRowFromSql( const char *sql,
                      char * const cVal[],
                      const int cValSize[],
                      int numOfCols,
                      const icatSessionStruct *icss );

int cmlGetOneRowFromSqlV2( const char *sql,
			   result_set *& resset,
                           const char *cVal[],
                           int maxCols,
                           const std::vector<std::string> &bindVars,
                           const icatSessionStruct *icss );

int cmlGetRowFromSqlV3( const char *sql,
                        char * const cVal[],
                        const int cValSize[],
                        int numOfCols,
                        const icatSessionStruct *icss );

int cmlFreeStatement( int statementNumber,
                      const icatSessionStruct *icss );

int cmlGetFirstRowFromSql( const char *sql,
                           int *statement,
                           const icatSessionStruct *icss );

int cmlGetFirstRowFromSqlBV( const char *sql,
                             const std::vector<std::string> &bindVars,
                             int *statement,
                             const icatSessionStruct *icss );

int cmlGetNextRowFromStatement( int stmtNum,
                                const icatSessionStruct *icss );

int cmlGetStringValueFromSql( const char *sql,
                              char *cVal,
                              int cValSize,
                              const std::vector<std::string> &bindVars,
                              const icatSessionStruct *icss );

int cmlGetStringValuesFromSql( const char *sql,
                               char * const cVal[],
                               const int cValSize[],
                               int numberOfStringsToGet,
                               const std::vector<std::string> &bindVars,
                               const icatSessionStruct *icss );

int cmlGetMultiRowStringValuesFromSql( const char *sql,
                                       char *returnedStrings,
                                       int maxStringLen,
                                       int maxNumberOfStringsToGet,
                                       const std::vector<std::string> &bindVars,
                                       const icatSessionStruct *icss );

int cmlGetIntegerValueFromSql( const char *sql,
                               rodsLong_t *iVal,
                               const std::vector<std::string> &bindVars,
                               const icatSessionStruct *icss );

int cmlGetIntegerValueFromSqlV3( const char *sql,
                                 rodsLong_t *iVal,
                                 const icatSessionStruct *icss );

int cmlCheckNameToken( const char *nameSpace,
                       const char *tokenName,
                       const icatSessionStruct *icss );

int cmlGetRowFromSingleTable( const char *tableName,
                              char * const cVal[],
                              const int cValSize[],
                              const char *selectCols[],
                              const char *whereCols[],
                              const char *whereConds[],
                              int numOfSels,
                              int numOfConds,
                              const icatSessionStruct *icss );

int cmlModifySingleTable( const char *tableName,
                          const char *updateCols[],
                          const char *updateValues[],
                          const char *whereColsAndConds[],
                          const char *whereValues[],
                          int numOfUpdates,
                          int numOfConds,
                          const icatSessionStruct *icss );

int cmlDeleteFromSingleTable( const char *tableName,
                              const char *selectCols[],
                              const char *selectConds[],
                              int numOfConds,
                              const icatSessionStruct *icss );

int cmlInsertIntoSingleTable( const char *tableName,
                              const char *insertCols[],
                              const char *insertValues[],
                              int numOfCols,
                              const icatSessionStruct *icss );

int cmlInsertIntoSingleTableV2( const char *tableName,
                                const char *insertCols,
                                const char *insertValues[],
                                int numOfCols,
                                const icatSessionStruct *icss );

int cmlGetOneRowFromSqlBV( const char *sql,
                           char * const cVal[],
                           const int cValSize[],
                           int numOfCols,
                           const std::vector<std::string> &bindVars,
                           const icatSessionStruct *icss );

int cmlGetOneRowFromSql( const char *sql,
                         char * const cVal[],
                         const int cValSize[],
                         int numOfCols,
                         const icatSessionStruct *icss );

rodsLong_t cmlGetNextSeqVal( const icatSessionStruct *icss );

int cmlGetNextSeqStr( char *seqStr, int maxSeqStrLen, const icatSessionStruct *icss );

rodsLong_t cmlCheckDir( const char *dirName, const char *userName, const char *userZone,
                        const char *accessLevel, const icatSessionStruct *icss );

rodsLong_t cmlCheckResc( const char *rescName, const char *userName, const char *userZone,
                         const char *accessLevel, const icatSessionStruct *icss );

rodsLong_t cmlCheckDirAndGetInheritFlag( const char *dirName, const char *userName,
        const char *userZone, const char *accessLevel,
        int *inheritFlag, const char *ticketStr, const char *ticketHost,
        const icatSessionStruct *icss );

rodsLong_t cmlCheckDirId( const char *dirId, const char *userName, const char *userZone,
                          const char *accessLevel, const icatSessionStruct *icss );

rodsLong_t cmlCheckDirOwn( const char *dirName, const char *userName, const char *userZone,
                           const icatSessionStruct *icss );

int cmlCheckDataObjId( const char *dataId, const char *userName, const char *zoneName,
                       const char *accessLevel, const char *ticketStr,
                       const char *ticketHost,
                       const icatSessionStruct *icss );

int cmlTicketUpdateWriteBytes( const char *ticketStr,
                               const char *dataSize, const char *objectId,
                               const icatSessionStruct *icss );

rodsLong_t cmlCheckDataObjOnly( const char *dirName, const char *dataName, const char *userName,
                                const char *userZone, const char *accessLevel, const icatSessionStruct *icss );

rodsLong_t cmlCheckDataObjOwn( const char *dirName, const char *dataName, const char *userName,
                               const char *userZone, const icatSessionStruct *icss );

int cmlCheckGroupAdminAccess( const char *userName, const char *userZone,
                              const char *groupName, const icatSessionStruct *icss );

int cmlGetGroupMemberCount( const char *groupName, const icatSessionStruct *icss );


#endif /* ICAT_MIDLEVEL_ROUTINES_H */
