/*
 * Main routine handling the event loop for the Lustre changelog reader and
 * the event loop for the iRODS API client.
 */


#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <stdbool.h>
#include <string.h>
#include <zmq.h>
#include <signal.h>
#include <thread>
#include <stdlib.h>

#include "irods_ops.h"
#include "rodsDef.h"
#include "lustre_change_table.hpp"
#include "config.h"

//#include "../../irods_lustre_api/src/inout_structs.h"
#include "inout_structs.h"
#include "logging.hpp"


#ifndef LPX64
#define LPX64   "%#llx"
#endif

#ifndef LCAP_CL_BLOCK
#define LCAP_CL_BLOCK   (0x01 << 1)
#endif

#define CHANGELOG_POLL_INTERVAL 5
#define UPDATE_IRODS_INTERVAL   9

struct lcap_cl_ctx;
//int lcap_changelog_start(struct lcap_cl_ctx **pctx, int flags, const char *mdtname, long long startrec);
//int lcap_changelog_fini(struct lcap_cl_ctx *ctx);

extern "C" {
    int start_lcap_changelog();
    int poll_change_log_and_process();
    int finish_lcap_changelog();
}

static volatile int keep_running = 1;

void interrupt_handler(int dummy) {
    keep_running = 0;
}

// irods api client thread main routine
void irods_api_client_main() {

    while (keep_running) {

        while (entries_ready_to_process()) {
            irodsLustreApiInp_t inp;
            memset( &inp, 0, sizeof( inp ) );
            write_change_table_to_capnproto_buf(&inp);
            send_change_map_to_irods(&inp);
            free(inp.buf);
        }
        sleep(UPDATE_IRODS_INTERVAL);
    }
    LOG(LOG_DBG,"irods_client_exiting\n");
}


int main(int argc, char **argv) {

    int  rc;
    const char *config_file = "lustre_irods_connector_config.json";
    char c;

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, interrupt_handler);

    while ((c = getopt (argc, argv, "c:l:h")) != -1) {
        switch (c) {
            case 'c':
                LOG(LOG_DBG,"setting configuration file to %s\n", optarg);
                config_file = optarg;
                break;
            case 'l':
                dbgstream = fopen(optarg, "a");
                if (dbgstream == NULL) {
                    dbgstream = stdout;
                    LOG(LOG_ERR, "could not open log file %s... using stdout instead.\n", optarg);
                } else {
                    LOG(LOG_DBG, "setting log file to %s\n", optarg);
                }
                break;
           case 'h':
           case '?':
                fprintf(stdout, "Usage: lustre_irods_connector [-c <configuration_file>] [-l <log_file>] [-h]\n");
                return 0;
        } 
    } 

    rc = read_config_file(config_file);
    if (rc < 0) {
        return 1;
    }

    rc = instantiate_irods_connection(); 
    if (rc < 0) {
        LOG(LOG_ERR, "instantiate_irods_connection failed.  exiting...\n");
        disconnect_irods_connection();
        return 1;
    }

    std::thread irods_api_client_thread(irods_api_client_main);

    rc = start_lcap_changelog();
    if (rc < 0) {
        LOG(LOG_ERR, "lcap_changelog_start: %s\n", zmq_strerror(-rc));
        disconnect_irods_connection();
        return 1;
    }

    while (keep_running) {
        LOG(LOG_INFO,"changelog client polling changelog\n");
        poll_change_log_and_process();
        sleep(CHANGELOG_POLL_INTERVAL);
    }

    // clean up before exit

    rc = finish_lcap_changelog();
    if (rc) {
        LOG(LOG_ERR, "lcap_changelog_fini: %s\n", zmq_strerror(-rc));
        disconnect_irods_connection();
        return 1;
    }

    disconnect_irods_connection();

    LOG(LOG_DBG,"changelog client exiting\n");

    if (dbgstream != stdout) {
        fclose(dbgstream);
    }

    return 0;
}
