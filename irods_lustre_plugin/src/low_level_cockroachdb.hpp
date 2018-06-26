/*** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***/
/*
  header file for the ODBC version of the icat low level routines,
  which is for Postgres or MySQL.
 */

#ifndef CLL_ODBC_HPP
#define CLL_ODBC_HPP

#include "sql.h"
#include "sqlext.h"

#include "rods.h"
#include "icatStructs.hpp"
#include "irods_error.hpp"

#include <vector>
#include <string>
#include <functional>
#include <libpq-fe.h>
#include <boost/variant.hpp>

#define MAX_BIND_VARS 32000

class result_set {
public:
  result_set();
  virtual ~result_set();
  virtual int next_row() = 0;
  virtual bool has_row();
  virtual const char *col_name(int _col);
  virtual int row_size();
  virtual int size();
  virtual void get_value(int _col, char *_buf, int _len);
  virtual const char *get_value(int _col);
  virtual void clear();
protected:
  PGresult *res_;
  int row_;
};

class all_result_set : public result_set {
public:
  all_result_set(std::function<int(PGresult *&)> _query);
  int next_row();
private:
  std::function<int(PGresult *&)> query_;
};

class paging_result_set : public result_set {
public:
  paging_result_set(std::function<int(int, int, PGresult *&)> _query, int _offset, int _maxrows);
  int next_row();
private:
  std::function<int(int, int, PGresult *&)> query_;
  int offset_;
  int maxrows_;
};

extern int cllBindVarCount;
extern const char *cllBindVars[MAX_BIND_VARS];
extern std::vector<result_set *> result_sets;
class _result_visitor : public boost::static_visitor<std::tuple<bool, irods::error>>
{
public:
  std::tuple<bool, irods::error> operator()(const irods::error &result) const;

  std::tuple<bool, irods::error> operator()(const std::tuple<bool, irods::error> & result) const;
};

irods::error execTx(const icatSessionStruct *icss, const boost::variant<std::function<irods::error()>, std::function<boost::variant<irods::error, std::tuple<bool, irods::error>>()>> &func);
int execSql(const icatSessionStruct *icss, result_set **_resset, const std::string &sql, const std::vector<std::string> &bindVars = std::vector<std::string>());
int execSql( const icatSessionStruct *icss, const std::string &sql, const std::vector<std::string> &bindVars = std::vector<std::string>());
int execSql( const icatSessionStruct *icss, result_set **_resset, const std::function<std::string(int, int)> &_sqlgen, const std::vector<std::string> &bindVars = std::vector<std::string>(), int offset = 0, int maxrows = 256);
int cllConnect( icatSessionStruct *icss, const std::string &host, int port, const std::string &dbname );
int cllDisconnect( icatSessionStruct *icss );
int cllExecSqlNoResult( const icatSessionStruct *icss, const char *sql );
int cllExecSqlWithResult( const icatSessionStruct *icss, int *stmtNum, const char *sql );
int cllExecSqlWithResultBV( const icatSessionStruct *icss, int *stmtNum, const char *sql,
                            const std::vector<std::string> &bindVars );
int cllGetBindVars(std::vector<std::string> &bindVars);
int cllFreeStatement(int _resinx);

#endif	/* CLL_ODBC_HPP */
