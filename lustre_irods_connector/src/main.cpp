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
#include "lustre_irods_errors.hpp"

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

namespace po = boost::program_options;

struct lcap_cl_ctx;

extern "C" {
    int start_lcap_changelog(const lustre_irods_connector_cfg_t*);
    int poll_change_log_and_process(const lustre_irods_connector_cfg_t*, void *change_map);
    int finish_lcap_changelog();
}

static char * s_recv_noblock(void *socket) {
    char buffer [256];
    int size = zmq_recv (socket, buffer, 255, ZMQ_NOBLOCK);
    if (size == -1)
        return nullptr;
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
void irods_api_client_main(std::shared_ptr<lustre_irods_connection> conn, const lustre_irods_connector_cfg_t *config_struct_ptr,
        change_map_t *change_map) {

    void *context = zmq_ctx_new ();
    void *subscriber = zmq_socket (context, ZMQ_SUB);
    zmq_connect (subscriber, "tcp://localhost:5563");
    zmq_setsockopt (subscriber, ZMQ_SUBSCRIBE, "changetable_readers", 1);

    // if we get an error sending data to irods, use a backoff_timer
    unsigned int error_backoff_timer = config_struct_ptr->update_irods_interval_seconds;

    bool irods_error_detected = false;

    while (true) {
        
        bool quit = false;

        LOG(LOG_INFO,"irods client reading change map\n");

        if (irods_error_detected) {
            LOG(LOG_INFO, "irods error detected.  try to reinstantiate the connection\n");

            if (conn->instantiate_irods_connection() == 0) {
               irods_error_detected = false;
            }
        } 

        while (!irods_error_detected && entries_ready_to_process(change_map)) {
            
            irodsLustreApiInp_t inp;
            memset( &inp, 0, sizeof( inp ) );

            std::shared_ptr<change_map_t> removed_entries = std::make_shared<change_map_t>(); 

            if (write_change_table_to_capnproto_buf(config_struct_ptr, &inp, change_map, removed_entries) < 0) {
                LOG(LOG_ERROR, "Could not execute write_change_table_to_capnproto_buf\n");
                continue;
            }
            if (conn->send_change_map_to_irods(&inp) == lustre_irods::IRODS_ERROR) {

                irods_error_detected = true;

                // error occurred - add removed_entries rows back into table 
                auto &change_map_seq = removed_entries->get<0>(); 
                for (auto iter = change_map_seq.begin(); iter != change_map_seq.end(); ++iter) {
                    change_map->push_back(*iter);
                }

                // next sleep will be twice the last sleep 
                if (error_backoff_timer < 256*config_struct_ptr->update_irods_interval_seconds) {
                    error_backoff_timer = error_backoff_timer << 1;
                }
            } else {
               error_backoff_timer = config_struct_ptr->update_irods_interval_seconds;
            }
            free(inp.buf);
        }

        // see if there is a quit message, if so terminate
        char *address = s_recv_noblock(subscriber);
        char *contents = s_recv_noblock(subscriber);
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

        sleep(error_backoff_timer);
    }

    zmq_close(subscriber);
    zmq_ctx_destroy(context);

    LOG(LOG_DBG,"irods_client_exiting\n");
}


int main(int argc, char *argv[]) {

    const char *config_file = "lustre_irods_connector_config.json";
    char *log_file = nullptr;

    signal(SIGPIPE, SIG_IGN);
    
    struct sigaction sa;
    memset( &sa, 0, sizeof(sa) );
    sa.sa_handler = interrupt_handler;
    sigfillset(&sa.sa_mask);
    sigaction(SIGINT,&sa,NULL);

    po::options_description desc("Allowed options");

    desc.add_options()
        ("help,h", "produce help message")
        ("config-file,c", po::value<std::string>(), "configuration file")
        ("log-file,l", po::value<std::string>(), "log file");
                                                                                            ;
    po::positional_options_description p;
    p.add("input-file", -1);
    
    po::variables_map vm;

    // read the command line arguments
    try {
        po::store(po::command_line_parser(argc, argv).options(desc).positional(p).run(), vm);
        po::notify(vm);

        if (vm.count("help")) {
            std::cout << "Usage:  lustre_irods_connector [options]" << std::endl;
            std::cout << desc << std::endl;
            return 0;
        }

        if (vm.count("config-file")) {
            LOG(LOG_DBG,"setting configuration file to %s\n", vm["config-file"].as<std::string>().c_str());
            config_file = vm["config-file"].as<std::string>().c_str();
        }

        if (vm.count("log-file")) {
            log_file = strdup(vm["log-file"].as<std::string>().c_str());
            dbgstream = fopen(log_file, "a");
            if (dbgstream == nullptr) {
                dbgstream = stdout;
                LOG(LOG_ERR, "could not open log file %s... using stdout instead.\n", optarg);
            } else {
                LOG(LOG_DBG, "setting log file to %s\n", vm["log-file"].as<std::string>().c_str());
            }
        }
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        std::cerr << desc << std::endl;
        return 1;
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

    // create the changemap in memory
    change_map_t change_map;

    // start the thread that sends changes to iRODS
    std::thread irods_api_client_thread(irods_api_client_main, conn, &config_struct, &change_map);

    rc = start_lcap_changelog(&config_struct);
    if (rc < 0) {
        LOG(LOG_ERR, "lcap_changelog_start: %s\n", zmq_strerror(-rc));
        return 1;
    }

    while (keep_running.load()) {

        LOG(LOG_INFO,"changelog client polling changelog\n");
        poll_change_log_and_process(&config_struct, &change_map);
        sleep(config_struct.changelog_poll_interval_seconds);
    }

    // send message to threads to terminate
    s_sendmore(publisher, "changetable_readers");
    s_send(publisher, "terminate");

    irods_api_client_thread.join();

    zmq_close(publisher);
    zmq_ctx_destroy(context);

    rc = finish_lcap_changelog();
    if (rc) {
        LOG(LOG_ERR, "lcap_changelog_fini: %s\n", zmq_strerror(-rc));
        return 1;
    }
    LOG(LOG_ERR, "finish_lcap_changelog exited normally\n");

    LOG(LOG_DBG,"changelog client exiting\n");
    if (dbgstream != stdout) {
        fclose(dbgstream);
    }

    if (log_file) {
        free(log_file);
    }

    return 0;
}
