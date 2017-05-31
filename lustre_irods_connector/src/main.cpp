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
#include <iostream>

#include "irods_ops.hpp"
#include "lustre_change_table.hpp"
#include "config.hpp"

#include "rodsDef.h"
#include "inout_structs.h"
#include "logging.hpp"

#include <boost/program_options.hpp>


#ifndef LPX64
#define LPX64   "%#llx"
#endif

#ifndef LCAP_CL_BLOCK
#define LCAP_CL_BLOCK   (0x01 << 1)
#endif

struct lcap_cl_ctx;

extern "C" {
    int start_lcap_changelog(const lustre_irods_connector_cfg_t*);
    int poll_change_log_and_process(const lustre_irods_connector_cfg_t*);
    int finish_lcap_changelog();
}

static char * s_recv_noblock(void *socket) {
    char buffer [256];
    int size = zmq_recv (socket, buffer, 255, ZMQ_NOBLOCK);
    if (size == -1)
        return NULL;
    buffer[size] = '\0';
    return strndup (buffer, sizeof(buffer) - 1);
}

//  Convert C string to 0MQ string and send to socket
static int s_send (void *socket, const char *string) {
    int size = zmq_send (socket, string, strlen (string), 0);
    return size;
}

//  Sends string as 0MQ string, as multipart non-terminal
static int s_sendmore (void *socket, const char *string) {
    int size = zmq_send (socket, string, strlen (string), ZMQ_SNDMORE);
    return size;
}


std::atomic<bool> keep_running(true);

void interrupt_handler(int dummy) {
    keep_running.store(false);
}

// irods api client thread main routine
// this is the main loop that reads the change entries in memory and sends them to iRODS via the API.
void irods_api_client_main(std::shared_ptr<lustre_irods_connection> conn, const lustre_irods_connector_cfg_t *config_struct_ptr) {

    void *context = zmq_ctx_new ();
    void *subscriber = zmq_socket (context, ZMQ_SUB);
    zmq_connect (subscriber, "tcp://localhost:5563");
    zmq_setsockopt (subscriber, ZMQ_SUBSCRIBE, "changetable_readers", 1);

    //while (keep_running.load()) {
    while (true) {

        bool quit = false;
        while (entries_ready_to_process()) {
            irodsLustreApiInp_t inp;
            memset( &inp, 0, sizeof( inp ) );
            write_change_table_to_capnproto_buf(config_struct_ptr, &inp);
            conn->send_change_map_to_irods(&inp);
            free(inp.buf);
        }

        // see if there is a quit message, if so terminate
        char *address = s_recv_noblock(subscriber);
        char *contents = s_recv_noblock(subscriber);
        printf("address and contents: %p %p\n", (void*)address, (void*)contents);
        if (address && contents && strcmp(contents, "terminate") == 0) {
            LOG(LOG_DBG, "irods client received a terminate message\n");
            quit = true;
        }
        if (address) 
            free(address);
        if (contents)
            free(contents);
        if (quit) 
            break;

        sleep(config_struct_ptr->update_irods_interval_seconds);
    }

    zmq_close (subscriber);
    zmq_ctx_destroy (context);

    LOG(LOG_DBG,"irods_client_exiting\n");
}


int main(int argc, char **argv) {

    const char *config_file = "lustre_irods_connector_config.json";
    char c;

    signal(SIGPIPE, SIG_IGN);
    
    struct sigaction sa;
    memset( &sa, 0, sizeof(sa) );
    sa.sa_handler = interrupt_handler;
    sigfillset(&sa.sa_mask);
    sigaction(SIGINT,&sa,NULL);


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
 
    int  rc;

    lustre_irods_connector_cfg_t config_struct;
    rc = read_config_file(config_file, &config_struct);
    if (rc < 0) {
        return 1;
    }

  
    std::shared_ptr<lustre_irods_connection> conn = std::make_shared<lustre_irods_connection>();

    rc = conn->instantiate_irods_connection(); 
    if (rc < 0) {
        LOG(LOG_ERR, "instantiate_irods_connection failed.  exiting...\n");
        return 1;
    }

    // read the resource id from resource name
    rc = conn->populate_irods_resc_id(&config_struct); 
    if (rc < 0) {
        LOG(LOG_ERR, "populate_irods_resc_id returned an error\n");
        return 1;
    }

    // start a pub/sub publisher which is used to terminate threads
    void *context = zmq_ctx_new ();
    void *publisher = zmq_socket (context, ZMQ_PUB);
    zmq_bind (publisher, "tcp://*:5563");

    // start the thread that sends changes to iRODS
    std::thread irods_api_client_thread(irods_api_client_main, conn, &config_struct);

    rc = start_lcap_changelog(&config_struct);
    if (rc < 0) {
        LOG(LOG_ERR, "lcap_changelog_start: %s\n", zmq_strerror(-rc));
        return 1;
    }

    while (keep_running.load()) {

        LOG(LOG_INFO,"changelog client polling changelog\n");
        poll_change_log_and_process(&config_struct);
        sleep(config_struct.changelog_poll_interval_seconds);
    }

    // send message to threads to terminate
    s_sendmore(publisher, "changetable_readers");
    s_send(publisher, "terminate");

    irods_api_client_thread.join();

    zmq_close(publisher);

    rc = finish_lcap_changelog();
    if (rc) {
        LOG(LOG_ERR, "lcap_changelog_fini: %s\n", zmq_strerror(-rc));
        return 1;
    }
    LOG(LOG_ERR, "finish_lcap_changelog exited normally\n");

    if (dbgstream != stdout) {
        fclose(dbgstream);
    }

    LOG(LOG_DBG,"changelog client exiting\n");
     
    return 0;
}
