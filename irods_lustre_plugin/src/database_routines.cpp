// =-=-=-=-=-=-=-
// irods includes
#include "apiHandler.hpp"
#include "irods_stacktrace.hpp"
#include "icatStructs.hpp"

#include "boost/lexical_cast.hpp"
#include <sql.h>
#include <sqlext.h>

// =-=-=-=-=-=-=-
// stl includes
#include <sstream>
#include <string>
#include <iostream>
#include <vector>

#define MAX_TOKEN 256
#define MAX_BIND_VARS 32000
#define TMP_STR_LEN 1040
#define SQL_INT_OR_LEN SQLLEN
#define SQL_UINT_OR_ULEN SQLULEN

int cllBindVarCount = 0;
const char *cllBindVars[MAX_BIND_VARS];
int cllBindVarCountPrev = 0;
const static SQLLEN GLOBAL_SQL_NTS = SQL_NTS;
SQLINTEGER columnLength[MAX_TOKEN];
SQLCHAR  psgErrorMsg[SQL_MAX_MESSAGE_LENGTH + 10];
static const short MAX_NUMBER_ICAT_COLUMS = 32;
static SQLLEN resultDataSizeArray[ MAX_NUMBER_ICAT_COLUMS ];

int cmlExecuteNoAnswerSql( const char *sql, icatSessionStruct *icss );
int cmlGetStringValueFromSql( const char *sql, char *cVal, int cValSize, std::vector<std::string> &bindVars, icatSessionStruct *icss );

#ifndef ORA_ICAT
static int didBegin = 0;
#endif
static int noResultRowCount = 0;



//#define MAX_BIND_VARS 32000
//extern const char *cllBindVars[MAX_BIND_VARS];
//extern int cllBindVarCount;

int logPsgError( int level, HENV henv, HDBC hdbc, HSTMT hstmt, int dbType ) {
    SQLCHAR         sqlstate[ SQL_SQLSTATE_SIZE + 10];
    SQLINTEGER sqlcode;
    SQLSMALLINT length;
    int errorVal = -2;
    while ( SQLError( henv, hdbc, hstmt, sqlstate, &sqlcode, psgErrorMsg,
                      SQL_MAX_MESSAGE_LENGTH + 1, &length ) == SQL_SUCCESS ) {
        if ( dbType == DB_TYPE_MYSQL ) {
            if ( strcmp( ( char * )sqlstate, "23000" ) == 0 &&
                    strstr( ( char * )psgErrorMsg, "Duplicate entry" ) ) {
                errorVal = CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME;
            }
        }
        else {
            if ( strstr( ( char * )psgErrorMsg, "duplicate key" ) ) {
                errorVal = CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME;
            }
        }
        rodsLog( level, "SQLSTATE: %s", sqlstate );
        rodsLog( level, "SQLCODE: %ld", sqlcode );
        rodsLog( level, "SQL Error message: %s", psgErrorMsg );
    }
    return errorVal;
}


void logBindVars(
    int level,
    std::vector<std::string> &bindVars ) {
    for ( std::size_t i = 0; i < bindVars.size(); i++ ) {
        if ( !bindVars[i].empty() ) {
            rodsLog( level, "bindVar%d=%s", i + 1, bindVars[i].c_str() );
        }
    }
}


void logTheBindVariables( int level ) {
    for ( int i = 0; i < cllBindVarCountPrev; i++ ) {
        char tmpStr[TMP_STR_LEN + 2];
        snprintf( tmpStr, TMP_STR_LEN, "bindVar[%d]=%s", i + 1, cllBindVars[i] );
        rodsLog( level, tmpStr );
    }
}


int _cllFreeStatementColumns( icatSessionStruct *icss, int statementNumber ) {

    icatStmtStrct * myStatement = icss->stmtPtr[statementNumber];

    for ( int i = 0; i < myStatement->numOfCols; i++ ) {
        free( myStatement->resultValue[i] );
        myStatement->resultValue[i] = NULL;
        free( myStatement->resultColName[i] );
        myStatement->resultColName[i] = NULL;
    }
    return 0;
}

int
cllGetRow( icatSessionStruct *icss, int statementNumber ) {
    icatStmtStrct *myStatement = icss->stmtPtr[statementNumber];

    for ( int i = 0; i < myStatement->numOfCols; i++ ) {
        strcpy( ( char * )myStatement->resultValue[i], "" );
    }
    SQLRETURN stat =  SQLFetch( myStatement->stmtPtr );
    if ( stat != SQL_SUCCESS && stat != SQL_NO_DATA_FOUND ) {
        rodsLog( LOG_ERROR, "cllGetRow: SQLFetch failed: %d", stat );
        return -1;
    }
    if ( stat == SQL_NO_DATA_FOUND ) {
        _cllFreeStatementColumns( icss, statementNumber );
        myStatement->numOfCols = 0;
    }
    return 0;
}



int cllFreeStatement( icatSessionStruct *icss, int statementNumber ) {

    icatStmtStrct * myStatement = icss->stmtPtr[statementNumber];
    if ( myStatement == NULL ) { /* already freed */
        return 0;
    }

    _cllFreeStatementColumns( icss, statementNumber );

    SQLRETURN stat = SQLFreeHandle( SQL_HANDLE_STMT, myStatement->stmtPtr );
    if ( stat != SQL_SUCCESS ) {
        rodsLog( LOG_ERROR, "cllFreeStatement SQLFreeHandle for statement error: %d", stat );
    }

    free( myStatement );

    icss->stmtPtr[statementNumber] = NULL; /* indicate that the statement is free */

    return 0;
}


static int cmp_stmt( const char *str1, const char *str2 ) {
    /* skip leading spaces */
    while ( isspace( *str1 ) ) {
        ++str1 ;
    }

    /* start comparing */
    for ( ; *str1 && *str2 ; ++str1, ++str2 ) {
        if ( tolower( *str1 ) != *str2 ) {
            return 0 ;
        }
    }

    /* skip trailing spaces */
    while ( isspace( *str1 ) ) {
        ++str1 ;
    }

    /* if we are at the end of the strings then they are equal */
    return *str1 == *str2 ;
}



#define maxPendingToRecord 5
#define pendingRecordSize 30
#define pBufferSize (maxPendingToRecord*pendingRecordSize)
int cllCheckPending( const char *sql, int option, int dbType ) {
    static int pendingCount = 0;
    static int pendingIx = 0;
    static int pendingAudits = 0;
    static char pBuffer[pBufferSize + 2];
    static int firstTime = 1;

    if ( firstTime ) {
        firstTime = 0;
        memset( pBuffer, 0, pBufferSize );
    }
    if ( option == 0 ) {
        if ( strncmp( sql, "commit", 6 ) == 0 ||
                strncmp( sql, "rollback", 8 ) == 0 ) {
            pendingIx = 0;
            pendingCount = 0;
            pendingAudits = 0;
            memset( pBuffer, 0, pBufferSize );
            return 0;
        }
        if ( pendingIx < maxPendingToRecord ) {
            strncpy( ( char * )&pBuffer[pendingIx * pendingRecordSize], sql,
                     pendingRecordSize - 1 );
            pendingIx++;
        }
        pendingCount++;
        return 0;
    }
    if ( option == 2 ) {
        pendingAudits++;
        return 0;
    }

    /* if there are some non-Audit pending SQL, log them */
    if ( pendingCount > pendingAudits ) {
        /* but ignore a single pending "begin" which can be normal */
        if ( pendingIx == 1 ) {
            if ( strncmp( ( char * )&pBuffer[0], "begin", 5 ) == 0 ) {
                return 0;
            }
        }
        if ( dbType == DB_TYPE_MYSQL ) {
            /* For mySQL, may have a few SET SESSION sql too, which we
               should ignore */
            int skip = 1;
            if ( strncmp( ( char * )&pBuffer[0], "begin", 5 ) != 0 ) {
                skip = 0;
            }
            int max = maxPendingToRecord;
            if ( pendingIx < max ) {
                max = pendingIx;
            }
            for ( int i = 1; i < max && skip == 1; i++ ) {
                if (    (strncmp((char *)&pBuffer[i*pendingRecordSize], "SET SESSION",   11) !=0)
                     && (strncmp((char *)&pBuffer[i*pendingRecordSize], "SET character", 13) !=0)) {
                    skip = 0;
                }
            }
            if ( skip ) {
                return 0;
            }
        }

        rodsLog( LOG_NOTICE, "Warning, pending SQL at cllDisconnect, count: %d",
                 pendingCount );
        int max = maxPendingToRecord;
        if ( pendingIx < max ) {
            max = pendingIx;
        }
        for ( int i = 0; i < max; i++ ) {
            rodsLog( LOG_NOTICE, "Warning, pending SQL: %s ...",
                     ( char * )&pBuffer[i * pendingRecordSize] );
        }
        if ( pendingAudits > 0 ) {
            rodsLog( LOG_NOTICE, "Warning, SQL will be commited with audits" );
        }
    }

    if ( pendingAudits > 0 ) {
        rodsLog( LOG_NOTICE,
                 "Notice, pending Auditing SQL committed at cllDisconnect" );
        return 1; /* tell caller (cllDisconect) to do a commit */
    }
    return 0;
}


int bindTheVariables( HSTMT myHstmt, const char *sql ) {

    int myBindVarCount = cllBindVarCount;
    cllBindVarCountPrev = cllBindVarCount; /* save in case we need to log error */
    cllBindVarCount = 0; /* reset for next call */

    for ( int i = 0; i < myBindVarCount; ++i ) {
        SQLRETURN stat = SQLBindParameter( myHstmt, i + 1, SQL_PARAM_INPUT, SQL_C_CHAR,
                                           SQL_CHAR, 0, 0, const_cast<char*>( cllBindVars[i] ), strlen( cllBindVars[i] ), const_cast<SQLLEN*>( &GLOBAL_SQL_NTS ) );
        char tmpStr[TMP_STR_LEN];
        snprintf( tmpStr, sizeof( tmpStr ), "bindVar[%d]=%s", i + 1, cllBindVars[i] );
        rodsLogSql( tmpStr );
        if ( stat != SQL_SUCCESS ) {
            rodsLog( LOG_ERROR,
                     "bindTheVariables: SQLBindParameter failed: %d", stat );
            return -1;
        }
    }

    return 0;
}


int _cllExecSqlNoResult(
    icatSessionStruct* icss,
    const char*        sql,
    int                option ) {
    rodsLog( LOG_DEBUG10, sql );

    HDBC myHdbc = icss->connectPtr;
    HSTMT myHstmt;
    SQLRETURN stat = SQLAllocHandle( SQL_HANDLE_STMT, myHdbc, &myHstmt );
    if ( stat != SQL_SUCCESS ) {
        rodsLog( LOG_ERROR, "_cllExecSqlNoResult: SQLAllocHandle failed for statement: %d", stat );
        return -1;
    }

    if ( option == 0 && bindTheVariables( myHstmt, sql ) != 0 ) {
        return -1;
    }

    rodsLogSql( sql );

    stat = SQLExecDirect( myHstmt, ( unsigned char * )sql, strlen( sql ) );
    SQL_INT_OR_LEN rowCount = 0;
    SQLRowCount( myHstmt, ( SQL_INT_OR_LEN * )&rowCount );
    switch ( stat ) {
    case SQL_SUCCESS:
        rodsLogSqlResult( "SUCCESS" );
        break;
    case SQL_SUCCESS_WITH_INFO:
        rodsLogSqlResult( "SUCCESS_WITH_INFO" );
        break;
    case SQL_NO_DATA_FOUND:
        rodsLogSqlResult( "NO_DATA" );
        break;
    case SQL_ERROR:
        rodsLogSqlResult( "SQL_ERROR" );
        break;
    case SQL_INVALID_HANDLE:
        rodsLogSqlResult( "HANDLE_ERROR" );
        break;
    default:
        rodsLogSqlResult( "UNKNOWN" );
    }

    int result;
    if ( stat == SQL_SUCCESS ||
            stat == SQL_SUCCESS_WITH_INFO ||
            stat == SQL_NO_DATA_FOUND ) {
        cllCheckPending( sql, 0, icss->databaseType );
        result = 0;
        if ( stat == SQL_NO_DATA_FOUND ) {
            result = CAT_SUCCESS_BUT_WITH_NO_INFO;
        }
        /* ODBC says that if statement is not UPDATE, INSERT, or DELETE then
           SQLRowCount may return anything. So for BEGIN, COMMIT and ROLLBACK
           we don't want to call it but just return OK.
        */
        if ( ! cmp_stmt( sql, "begin" )  &&
                ! cmp_stmt( sql, "commit" ) &&
                ! cmp_stmt( sql, "rollback" ) ) {
            /* Doesn't seem to return SQL_NO_DATA_FOUND, so check */
            rowCount = 0;
            if ( SQLRowCount( myHstmt, ( SQL_INT_OR_LEN * )&rowCount ) ) {
                /* error getting rowCount???, just call it no_info */
                result = CAT_SUCCESS_BUT_WITH_NO_INFO;
            }
            if ( rowCount == 0 ) {
                result = CAT_SUCCESS_BUT_WITH_NO_INFO;
            }
        }
    }
    else {
        if ( option == 0 ) {
            logTheBindVariables( LOG_NOTICE );
        }
        rodsLog( LOG_NOTICE, "_cllExecSqlNoResult: SQLExecDirect error: %d sql:%s",
                 stat, sql );
        result = logPsgError( LOG_NOTICE, icss->environPtr, myHdbc, myHstmt,
                              icss->databaseType );
    }

    stat = SQLFreeHandle( SQL_HANDLE_STMT, myHstmt );
    if ( stat != SQL_SUCCESS ) {
        rodsLog( LOG_ERROR, "_cllExecSqlNoResult: SQLFreeHandle for statement error: %d", stat );
    }

    noResultRowCount = rowCount;

    return result;
}


int cllExecSqlNoResult( icatSessionStruct *icss, const char *sql ) {

#ifndef ORA_ICAT
    if ( strncmp( sql, "commit", 6 ) == 0 ||
            strncmp( sql, "rollback", 8 ) == 0 ) {
        didBegin = 0;
    }
    else {
        if ( didBegin == 0 ) {
            int status = _cllExecSqlNoResult( icss, "begin", 1 );
            if ( status != SQL_SUCCESS ) {
                return status;
            }
        }
        didBegin = 1;
    }
#endif
    return _cllExecSqlNoResult( icss, sql, 0 );
}

int cllExecSqlWithResultBV( icatSessionStruct *icss, int *stmtNum, 
    const char *sql, std::vector< std::string > &bindVars ) {

    rodsLog( LOG_DEBUG10, sql );

    HDBC myHdbc = icss->connectPtr;
    HSTMT hstmt;
    SQLRETURN stat = SQLAllocHandle( SQL_HANDLE_STMT, myHdbc, &hstmt );
    if ( stat != SQL_SUCCESS ) {
        rodsLog( LOG_ERROR, "cllExecSqlWithResultBV: SQLAllocHandle failed for statement: %d",
                 stat );
        return -1;
    }

    int statementNumber = -1;
    for ( int i = 0; i < MAX_NUM_OF_CONCURRENT_STMTS && statementNumber < 0; i++ ) {
        if ( icss->stmtPtr[i] == 0 ) {
            statementNumber = i;
        }
    }
    if ( statementNumber < 0 ) {
        rodsLog( LOG_ERROR,
                 "cllExecSqlWithResultBV: too many concurrent statements" );
        return CAT_STATEMENT_TABLE_FULL;
    }

    icatStmtStrct * myStatement = ( icatStmtStrct * )malloc( sizeof( icatStmtStrct ) );
    icss->stmtPtr[statementNumber] = myStatement;

    myStatement->stmtPtr = hstmt;

    for ( std::size_t i = 0; i < bindVars.size(); i++ ) {
        if ( !bindVars[i].empty() ) {

            stat = SQLBindParameter( hstmt, i + 1, SQL_PARAM_INPUT, SQL_C_CHAR,
                                     SQL_CHAR, 0, 0, const_cast<char*>( bindVars[i].c_str() ), bindVars[i].size(), const_cast<SQLLEN*>( &GLOBAL_SQL_NTS ) );
            char tmpStr[TMP_STR_LEN];
            snprintf( tmpStr, sizeof( tmpStr ), "bindVar%ju=%s", static_cast<uintmax_t>(i + 1), bindVars[i].c_str() );
            rodsLogSql( tmpStr );
            if ( stat != SQL_SUCCESS ) {
                rodsLog( LOG_ERROR,
                         "cllExecSqlWithResultBV: SQLBindParameter failed: %d", stat );
                return -1;
            }
        }
    }
    rodsLogSql( sql );
    stat = SQLExecDirect( hstmt, ( unsigned char * )sql, strlen( sql ) );

    switch ( stat ) {
    case SQL_SUCCESS:
        rodsLogSqlResult( "SUCCESS" );
        break;
    case SQL_SUCCESS_WITH_INFO:
        rodsLogSqlResult( "SUCCESS_WITH_INFO" );
    case SQL_NO_DATA_FOUND:
        rodsLogSqlResult( "NO_DATA" );
        break;
    case SQL_ERROR:
        rodsLogSqlResult( "SQL_ERROR" );
        break;
    case SQL_INVALID_HANDLE:
        rodsLogSqlResult( "HANDLE_ERROR" );
        break;
    default:
        rodsLogSqlResult( "UNKNOWN" );
    }

    if ( stat != SQL_SUCCESS &&
            stat != SQL_SUCCESS_WITH_INFO &&
            stat != SQL_NO_DATA_FOUND ) {
        logBindVars( LOG_NOTICE, bindVars );
        rodsLog( LOG_NOTICE,
                 "cllExecSqlWithResultBV: SQLExecDirect error: %d, sql:%s",
                 stat, sql );
        logPsgError( LOG_NOTICE, icss->environPtr, myHdbc, hstmt,
                     icss->databaseType );
        return -1;
    }

    SQLSMALLINT numColumns;
    stat = SQLNumResultCols( hstmt, &numColumns );
    if ( stat != SQL_SUCCESS ) {
        rodsLog( LOG_ERROR, "cllExecSqlWithResultBV: SQLNumResultCols failed: %d",
                 stat );
        return -2;
    }
    myStatement->numOfCols = numColumns;

    for ( int i = 0; i < numColumns; i++ ) {
        SQLCHAR colName[MAX_TOKEN] = "";
        SQLSMALLINT colNameLen;
        SQLSMALLINT colType;
        SQL_UINT_OR_ULEN precision;
        SQLSMALLINT scale;
        stat = SQLDescribeCol( hstmt, i + 1, colName, sizeof( colName ),
                               &colNameLen, &colType, &precision, &scale, NULL );
        if ( stat != SQL_SUCCESS ) {
            rodsLog( LOG_ERROR, "cllExecSqlWithResultBV: SQLDescribeCol failed: %d",
                     stat );
            return -3;
        }
        /*  printf("colName='%s' precision=%d\n",colName, precision); */
        columnLength[i] = precision;
        SQL_INT_OR_LEN  displaysize;
        stat = SQLColAttribute( hstmt, i + 1, SQL_COLUMN_DISPLAY_SIZE,
                                NULL, 0, NULL, &displaysize ); // JMC :: changed to SQLColAttribute for odbc update
        if ( stat != SQL_SUCCESS ) {
            rodsLog( LOG_ERROR,
                     "cllExecSqlWithResultBV: SQLColAttributes failed: %d",
                     stat );
            return -3;
        }

        if ( displaysize > ( ( int )strlen( ( char * ) colName ) ) ) {
            columnLength[i] = displaysize + 1;
        }
        else {
            columnLength[i] = strlen( ( char * ) colName ) + 1;
        }
        /*      printf("columnLength[%d]=%d\n",i,columnLength[i]); */

        myStatement->resultValue[i] = ( char* )malloc( ( int )columnLength[i] );

        myStatement->resultValue[i][0] = '\0';
        // =-=-=-=-=-=-=-
        // JMC :: added static array to catch the result set size.  this was necessary to
        stat = SQLBindCol( hstmt, i + 1, SQL_C_CHAR, myStatement->resultValue[i], columnLength[i], &resultDataSizeArray[i] );

        if ( stat != SQL_SUCCESS ) {
            rodsLog( LOG_ERROR,
                     "cllExecSqlWithResultBV: SQLColAttributes failed: %d",
                     stat );
            return -4;
        }


        myStatement->resultColName[i] = ( char* )malloc( ( int )columnLength[i] );
#ifdef ORA_ICAT
        //oracle prints column names (which are case-insensitive) in upper case,
        //so to remain consistent with postgres and mysql, we convert them to lower case.
        for ( int j = 0; j < columnLength[i] && colName[j] != '\0'; j++ ) {
            colName[j] = tolower( colName[j] );
        }
#endif
        strncpy( myStatement->resultColName[i], ( char * )colName, columnLength[i] );

    }
    *stmtNum = statementNumber;
    return 0;
}




int cmlExecuteNoAnswerSql( const char *sql,
                           icatSessionStruct *icss ) {
    int i;
    i = cllExecSqlNoResult( icss, sql );
    if ( i ) {
        if ( i <= CAT_ENV_ERR ) {
            return ( i );   /* already an iRODS error code */
        }
        return CAT_SQL_ERR;
    }
    return 0;

}

int cmlGetOneRowFromSqlBV( const char *sql,
                           char *cVal[],
                           int cValSize[],
                           int numOfCols,
                           std::vector<std::string> &bindVars,
                           icatSessionStruct *icss ) {
    int stmtNum;
    char updatedSql[MAX_SQL_SIZE + 1];

#ifdef ORA_ICAT
    strncpy( updatedSql, sql, MAX_SQL_SIZE );
    updatedSql[MAX_SQL_SIZE] = '\0';
#else
    strncpy( updatedSql, sql, MAX_SQL_SIZE );
    updatedSql[MAX_SQL_SIZE] = '\0';
    /* Verify there no limit or offset statement */
    if ( ( strstr( updatedSql, "limit " ) == NULL ) && ( strstr( updatedSql, "offset " ) == NULL ) ) {
        /* add 'limit 1' for performance improvement */
        strncat( updatedSql, " limit 1", MAX_SQL_SIZE );
        rodsLog( LOG_DEBUG10, "cmlGetOneRowFromSqlBV %s", updatedSql );
    }
#endif
    int status = cllExecSqlWithResultBV( icss, &stmtNum, updatedSql,
                                         bindVars );
    if ( status != 0 ) {
        if ( status <= CAT_ENV_ERR ) {
            return status;    /* already an iRODS error code */
        }
        return CAT_SQL_ERR;
    }

    if ( cllGetRow( icss, stmtNum ) != 0 )  {
        cllFreeStatement( icss, stmtNum );
        return CAT_GET_ROW_ERR;
    }
    if ( icss->stmtPtr[stmtNum]->numOfCols == 0 ) {
        cllFreeStatement( icss, stmtNum );
        return CAT_NO_ROWS_FOUND;
    }
    int numCVal = std::min( numOfCols, icss->stmtPtr[stmtNum]->numOfCols );
    for ( int j = 0; j < numCVal ; j++ ) {
        rstrcpy( cVal[j], icss->stmtPtr[stmtNum]->resultValue[j], cValSize[j] );
    }

    cllFreeStatement( icss, stmtNum );
    return numCVal;

}


int cmlGetStringValueFromSql( const char *sql,
                              char *cVal,
                              int cValSize,
                              std::vector<std::string> &bindVars,
                              icatSessionStruct *icss ) {
    int status;
    char *cVals[2];
    int iVals[2];

    cVals[0] = cVal;
    iVals[0] = cValSize;

    status = cmlGetOneRowFromSqlBV( sql, cVals, iVals, 1,
                                    bindVars, icss );
    if ( status == 1 ) {
        return 0;
    }
    else {
        return status;
    }

}

int cmlGetIntegerValueFromSql( const char *sql,
                               rodsLong_t *iVal,
                               std::vector<std::string> &bindVars,
                               icatSessionStruct *icss ) {
    int i, cValSize;
    char *cVal[2];
    char cValStr[MAX_INTEGER_SIZE + 10];

    cVal[0] = cValStr;
    cValSize = MAX_INTEGER_SIZE;

    i = cmlGetOneRowFromSqlBV( sql, cVal, &cValSize, 1,
                               bindVars, icss );
    if ( i == 1 ) {
        if ( *cVal[0] == '\0' ) {
            return CAT_NO_ROWS_FOUND;
        }
        *iVal = strtoll( *cVal, NULL, 0 );
        return 0;
    }
    return i;
}


rodsLong_t cmlGetCurrentSeqVal( icatSessionStruct *icss ) {

    int status;
    rodsLong_t seq_no;

#ifdef ORA_ICAT
    std::string sql = "select r_objectid.nextval from dual";
#elif MY_ICAT
    std::string sql = "select r_objectid.nextval()";
#else
    std::string sql = "select nextval('r_objectid')";
#endif

    std::vector<std::string> emptyBindVars;
    status = cmlGetIntegerValueFromSql(sql.c_str(), &seq_no, emptyBindVars, icss);
    if ( status < 0 ) {
        rodsLog(LOG_NOTICE, "cmlGetCurrentSeqVal cmlGetIntegerValueFromSql failure %d", status);
        return status;
    }
    return seq_no;
}

