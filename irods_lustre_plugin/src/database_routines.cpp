// =-=-=-=-=-=-=-
// irods includes
#include "apiHandler.hpp"
#include "irods_stacktrace.hpp"
#include "icatStructs.hpp"

#if defined(COCKROACHDB_ICAT)
  #include "mid_level_cockroachdb.hpp"
  #include "low_level_cockroachdb.hpp"
#else
  #include "mid_level_other.hpp"
  #include "low_level_odbc_other.hpp"
#endif

#include "database_routines.hpp"

#include "boost/lexical_cast.hpp"
#include <sql.h>
#include <sqlext.h>

// =-=-=-=-=-=-=-
// stl includes
#include <sstream>
#include <string>
#include <iostream>
#include <vector>

#if defined (MY_ICAT)

rodsLong_t cmlGetCurrentSeqValMySQL( icatSessionStruct *icss ) {

    int status;
    rodsLong_t seq_no;

    std::string sql = "select R_ObjectId_nextval()";

    std::vector<std::string> emptyBindVars;
    status = cmlGetIntegerValueFromSql(sql.c_str(), &seq_no, emptyBindVars, icss);
    if ( status < 0 ) {
        rodsLog(LOG_NOTICE, "cmlGetCurrentSeqValMySQL cmlGetIntegerValueFromSql failure %d", status);
        return status;
    }
    return seq_no;
}


#endif

#if !defined(COCKROACHDB_ICAT)

    int cmlGetNSeqVals( icatSessionStruct *icss, size_t n, std::vector<rodsLong_t>& sequences ) {

        int status;

    #if defined(ORA_ICAT)
        std::string sql = "select r_objectid.nextval from ( select level from dual connect by level < " +
            std::to_string(n) + ")";
    #elif defined(MY_ICAT)
        std::string sql = "select R_ObjectId_nextval()";
    #else
        std::string sql = "select nextval('r_objectid') from generate_series(1, " + std::to_string(n) + ")";
    #endif

        std::vector<std::string> emptyBindVars;

    #if defined(MY_ICAT)

        // for mysql, the query must be executed n times
         for (size_t i = 0; i < n; ++i) {

            status =  cmlGetCurrentSeqValMySQL(icss); 
            if ( status < 0 ) {
                rodsLog(LOG_ERROR, "cmlGetNSeqVals cmlGetCurrentSeqVal failure %d", status);
                return status;
            }

            sequences.push_back(status);

        }

    #else
        int stmt_num;
        for (size_t i = 0; i < n; ++i) {

            if (i == 0) {
                status = cmlGetFirstRowFromSqlBV(sql.c_str(), emptyBindVars, &stmt_num, icss);
            } else {
                status = cmlGetNextRowFromStatement( stmt_num, icss );
            }
     
            if ( status < 0 ) {
                rodsLog(LOG_ERROR, "cmlGetNSeqVals cmlGetFirstRowFromSqlBV/cmlGetNextRowFromStatement failure %d", status);
                cllFreeStatement(icss, stmt_num); 
                return status;
            }

            size_t nCols = icss->stmtPtr[stmt_num]->numOfCols;
            if (nCols != 1) {
                rodsLog(LOG_ERROR, "cmlGetNSeqVals unexpected number of columns %d", nCols);
                cllFreeStatement(icss, stmt_num); 
                return CAT_SQL_ERR;
            }

            char *result_str = icss->stmtPtr[stmt_num]->resultValue[0];
            rodsLong_t result = 0;
            try {
               result = boost::lexical_cast<rodsLong_t>(result_str);
            } catch (boost::bad_lexical_cast& e) {
                rodsLog(LOG_ERROR, "cmlGetNSeqVals unexpected value returned %s", result_str);
                cllFreeStatement(icss, stmt_num); 
                return CAT_SQL_ERR;
            }

            sequences.push_back(result);

        }

                cllFreeStatement(icss, stmt_num); 

    #endif

        return 0;

    }
#endif   // !defined(COCKROACHDB_ICAT)


#if defined(COCKROACHDB_ICAT)

    int cmlGetNSeqVals( icatSessionStruct *icss, size_t n, std::vector<rodsLong_t>& sequences ) {

        int status;

        std::string sql = "insert into r_objectid values ";
        for (size_t i = 0; i < n; ++i) {
            sql += "(unique_rowid())";
            if (i < n-1) {
                sql += ",";
            } 
        }
        sql += " returning object_id";

        std::vector<std::string> emptyBindVars;

        int stmt_num;
        for (size_t i = 0; i < n; ++i) {

            if (i == 0) {
                status = cmlGetFirstRowFromSqlBV(sql.c_str(), emptyBindVars, &stmt_num, icss);
            } else {
                status = cmlGetNextRowFromStatement( stmt_num, icss );
            }
     
            if ( status < 0 ) {
                rodsLog(LOG_ERROR, "cmlGetNSeqVals cmlGetFirstRowFromSqlBV/cmlGetNextRowFromStatement failure %d", status);
                cllFreeStatement(stmt_num); 
                return status;
            }

            //size_t nCols = icss->stmtPtr[stmt_num]->numOfCols;
            size_t nCols = result_sets[stmt_num]->row_size();
            if (nCols != 1) {
                rodsLog(LOG_ERROR, "cmlGetNSeqVals unexpected number of columns %d", nCols);
                cllFreeStatement(stmt_num); 
                return CAT_SQL_ERR;
            }

            const char *result_str = result_sets[stmt_num]->get_value(0);
            rodsLong_t result = 0;
            try {
               result = boost::lexical_cast<rodsLong_t>(result_str);
            } catch (boost::bad_lexical_cast& e) {
                rodsLog(LOG_ERROR, "cmlGetNSeqVals unexpected value returned %s", result_str);
                cllFreeStatement(stmt_num); 
                return CAT_SQL_ERR;
            }

            sequences.push_back(result);

        }

        cllFreeStatement(stmt_num); 

        return 0;

    }

#endif   // defined(COCKROADDB_ICAT)

    
#if defined(MY_ICAT)

void setMysqlIsolationLevelReadCommitted(icatSessionStruct *icss) {
    cllBindVarCount = 0;
    int status = cmlExecuteNoAnswerSql("set SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", icss);
    if (status != 0 && status != CAT_SUCCESS_BUT_WITH_NO_INFO) {
        rodsLog(LOG_ERROR, "Error setting mysql isolation level.  Error is %i.", status);
        return;
    }
}

#endif
