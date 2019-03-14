#ifndef LUSTRE_IRODS_ERRORS_HPP
#define LUSTRE_IRODS_ERRORS_HPP

namespace lustre_irods {

const int SUCCESS = 0;
const int INVALID_OPERAND_ERROR = -1;
const int IRODS_ERROR = -2;
const int IRODS_CONNECTION_ERROR = -3;
const int CONFIGURATION_ERROR = -4;
const int RESOURCE_NOT_FOUND_ERROR = -5;
const int INVALID_RESOURCE_ID_ERROR = -6;
const int IRODS_ENVIRONMENT_ERROR = -7;
const int LUSTRE_OBJECT_DNE_ERROR = -8;
const int LLAPI_FID2PATH_ERROR = -9;
const int INVALID_CR_TYPE_ERROR = -10;
const int SQLITE_DB_ERROR = -11;
const int QUIT = -12;
const int COLLISION_IN_FIDSTR = -13;
const int CHANGELOG_START_ERROR = -14;

}

#endif
