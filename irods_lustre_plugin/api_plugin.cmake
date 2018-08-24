
set(
  IRODS_DATABASE_PLUGIN_COMPILE_DEFINITIONS_postgres
  POSTGRES_ICAT
  )
set(
  IRODS_DATABASE_PLUGIN_COMPILE_DEFINITIONS_mysql
  MY_ICAT
  )
set(
  IRODS_DATABASE_PLUGIN_COMPILE_DEFINITIONS_oracle
  ORA_ICAT
  )
set(
  IRODS_DATABASE_PLUGIN_COMPILE_DEFINITIONS_cockroachdb
  COCKROACHDB_ICAT
  )

#set(
#  IRODS_DATABASE_PLUGINS
#  postgres
#  mysql
#  oracle
#  )


set(
  IRODS_API_PLUGIN_SOURCES_lustre_api_server
  ${CMAKE_SOURCE_DIR}/src/libirods-lustre-api.cpp
  ${CMAKE_SOURCE_DIR}/src/database_routines.cpp
  ${CMAKE_SOURCE_DIR}/src/irods_lustre_operations.cpp
  ${CMAKE_SOURCE_DIR}/../lustre_irods_connector/src/change_table.capnp.h
  )

set(
  IRODS_API_PLUGIN_SOURCES_lustre_api_client
  ${CMAKE_SOURCE_DIR}/src/libirods-lustre-api.cpp
  ${CMAKE_SOURCE_DIR}/src/database_routines.cpp
  ${CMAKE_SOURCE_DIR}/src/irods_lustre_operations.cpp
  ${CMAKE_SOURCE_DIR}/../lustre_irods_connector/src/change_table.capnp.h
  )

set(
  IRODS_API_PLUGIN_COMPILE_DEFINITIONS_lustre_api_server
  RODS_SERVER
  ENABLE_RE
  )

set(
  IRODS_API_PLUGIN_COMPILE_DEFINITIONS_lustre_api_client
  )

set(
  IRODS_API_PLUGIN_LINK_LIBRARIES_lustre_api_server
  irods_client
  irods_server
  irods_common
  irods_plugin_dependencies
  )

set(
  IRODS_API_PLUGIN_LINK_LIBRARIES_lustre_api_client
  irods_client
  irods_server
  irods_common
  irods_plugin_dependencies
  )

set(
  IRODS_API_PLUGINS
  lustre_api_server
  lustre_api_client
  )

foreach(PLUGIN ${IRODS_API_PLUGINS})
    #foreach (DB_TYPE ${IRODS_DATABASE_PLUGINS})
  add_library(
    ${PLUGIN}_${DB_TYPE}
    MODULE
    ${IRODS_API_PLUGIN_SOURCES_${PLUGIN}}
    )

  target_include_directories(
    ${PLUGIN}_${DB_TYPE}
    PRIVATE
    /usr/include
    ${IRODS_INCLUDE_DIRS}
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/include
    ${IRODS_EXTERNALS_FULLPATH_JANSSON}/include
    ${IRODS_EXTERNALS_FULLPATH_ARCHIVE}/include
    )

  target_link_libraries(
    ${PLUGIN}_${DB_TYPE}
    PRIVATE
    ${IRODS_API_PLUGIN_LINK_LIBRARIES_${PLUGIN}}
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_system.so
    ${IRODS_EXTERNALS_FULLPATH_ARCHIVE}/lib/libarchive.so
    ${OPENSSL_CRYPTO_LIBRARY}
    /usr/lib/irods/plugins/database/lib${DB_TYPE}.so
    /usr/local/lib/libcapnp.so
    /usr/local/lib/libkj.so
    ${ODBC_LIBRARY}
    )

  target_compile_definitions(${PLUGIN}_${DB_TYPE} PRIVATE ${IRODS_DATABASE_PLUGIN_COMPILE_DEFINITIONS_${DB_TYPE}} ${IRODS_API_PLUGIN_COMPILE_DEFINITIONS_${PLUGIN}} ${IRODS_COMPILE_DEFINITIONS} BOOST_SYSTEM_NO_DEPRECATED)
  target_compile_options(${PLUGIN}_${DB_TYPE} PRIVATE -Wno-write-strings)
  set_property(TARGET ${PLUGIN}_${DB_TYPE} PROPERTY CXX_STANDARD ${IRODS_CXX_STANDARD})

  install(
    TARGETS
    ${PLUGIN}_${DB_TYPE}
    COMPONENT ${DB_TYPE}
    LIBRARY
    DESTINATION usr/lib/irods/plugins/api
    )
#endforeach()
endforeach()
