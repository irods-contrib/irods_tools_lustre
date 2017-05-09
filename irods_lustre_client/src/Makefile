LCAP_ROOT=/lcap

INC=-I/opt/irods-externals/clang3.8-0/include/c++/v1 -I/usr/include/irods -I/opt/irods-externals/boost1.60.0-0/include -I/opt/irods-externals/cppzmq4.1-0/include -I/opt/irods-externals/zeromq4-14.1.3-0/include -I. -I$(LCAP_ROOT)/src/include -I$(LCAP_ROOT)/client

#CPP_OPT=-DBOOST_SYSTEM_NO_DEPRECATED -DRODS_SERVER -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -D_LARGEFILE_SOURCE -D_LARGE_FILES -Dlinux_platform -O3 -DNDEBUG -nostdinc++ -Wall -Wextra -Werror -Wno-unused-parameter -Wno-write-strings -std=gnu++14 

CPP_OPT=-DBOOST_SYSTEM_NO_DEPRECATED -DRODS_SERVER -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -D_LARGEFILE_SOURCE -D_LARGE_FILES -Dlinux_platform -g -O0 -DNDEBUG -nostdinc++ -Wall -Wextra -Werror -Wno-unused-parameter -Wno-write-strings -std=gnu++14 

LINK_OPT=-DNDEBUG -stdlib=libc++ -lc++abi -Wl,-rpath,/opt/irods-externals/clang-runtime3.8-0/lib:/opt/irods-externals/boost1.60.0-0/lib:/opt/irods-externals/zeromq4-14.1.3-0/lib -ldl -lpthread -lrt

LIBS=/usr/lib/libirods_client.so.4.2.0 /usr/lib/libirods_plugin_dependencies.so.4.2.0 /usr/lib/libirods_common.so.4.2.0 /opt/irods-externals/boost1.60.0-0/lib/libboost_filesystem.so /opt/irods-externals/boost1.60.0-0/lib/libboost_program_options.so /opt/irods-externals/boost1.60.0-0/lib/libboost_system.so /opt/irods-externals/zeromq4-14.1.3-0/lib/libzmq.so /opt/irods-externals/boost1.60.0-0/lib/libboost_thread.so 

LIB_DIRS=-L/opt/irods-externals/zeromq4-14.1.3-0/lib 

CLANG_CPP=/opt/irods-externals/clang3.8-0/bin/clang++
CLANG_C=/opt/irods-externals/clang3.8-0/bin/clang

all: irods_lustre_connector #test_change_table

lustre_change_table.o: lustre_change_table.cpp lustre_change_table.hpp 
	$(CLANG_CPP) $(CPP_OPT) $(INC) -o lustre_change_table.o -c lustre_change_table.cpp -nostdinc++

irods_ops.o: irods_ops.cpp irods_ops.h
	$(CLANG_CPP) $(CPP_OPT)  $(INC) -o irods_ops.o -c irods_ops.cpp

changelog_config.o: changelog_config.cpp changelog_config.h
	$(CLANG_CPP) $(CPP_OPT)  $(INC) -o changelog_config.o -c changelog_config.cpp

changelog_poller.o: changelog_poller.c irods_ops.h changelog_config.h
	$(CLANG_C) $(INC) -DHAVE_CONFIG_H -g -O0 -c -nostdinc++ -o changelog_poller.o -c changelog_poller.c

#test_change_table.o: test_change_table.cpp lustre_change_table.hpp
#	 $(CLANG_CPP) $(CPP_OPT) $(INC) -o test_change_table.o -c test_change_table.cpp

main.o: main.cpp
	$(CLANG_CPP) $(CPP_OPT) $(INC) -o main.o -c main.cpp 

#test_change_table: lustre_change_table.o test_change_table.o irods_ops.o 
#	$(CLANG_CPP) $(LIBS) -g -O2 -o test_change_table test_change_table.o irods_ops.o lustre_change_table.o $(LINK_OPT) -llustreapi $(LIB_DIRS) $(LCAP_ROOT)/src/common/.libs/liblcapcommon.a $(LCAP_ROOT)/src/client/.libs/liblcap.so $(LINK_OPT) -Wl,-rpath -Wl,$(LCAP_ROOT)/src/client/.libs
#	$(CLANG_CPP) $(LIBS) -g -O2 -o test_change_table changelog_poller.o lustre_change_table.o $(LINK_OPT) -llustreapi $(LIB_DIRS) $(LCAP_ROOT)/src/common/.libs/liblcapcommon.a $(LCAP_ROOT)/src/client/.libs/liblcap.so $(LINK_OPT) -Wl,-rpath -Wl,$(LCAP_ROOT)/src/client/.libs -lzmq
#	$(CLANG_CPP) $(LIBS) -g -O2 -o test_change_table lustre_change_table.o test_change_table.o $(LIB_DIRS) $(LINK_OPT) -Wl,-rpath 
#	$(CLANG_CPP) $(LINK_OP) $(LIBS) test_change_table.o lustre_change_table.o -o test_change_table


irods_lustre_connector: main.o changelog_config.o changelog_poller.o irods_ops.o lustre_change_table.o
	$(CLANG_CPP) $(LIBS) -g -O0 -o irods_lustre_connector main.o changelog_config.o changelog_poller.o irods_ops.o lustre_change_table.o $(LINK_OPT) -llustreapi $(LIB_DIRS) $(LCAP_ROOT)/src/common/.libs/liblcapcommon.a $(LCAP_ROOT)/src/client/.libs/liblcap.so $(LINK_OPT) -Wl,-rpath -Wl,$(LCAP_ROOT)/src/client/.libs -lzmq 


clean:
	@rm -f ./*.o test irods_lustre_connector 





