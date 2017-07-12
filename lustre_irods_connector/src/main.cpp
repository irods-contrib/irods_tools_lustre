/*
 * Main routine handling the event loop for the Lustre changelog reader and
 * the event loop for the iRODS API client.
 */


#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <stdbool.h>
#include <string.h>
#include <zmq.hpp>

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


//  Sends string as 0MQ string, as multipart non-terminal 
static bool s_sendmore (zmq::socket_t & socket, const std::string & string) {

    zmq::message_t message(string.size());
    memcpy (message.data(), string.data(), string.size());

    bool rc = socket.send (message, ZMQ_SNDMORE);
    return (rc);
}

//  Receive 0MQ string from socket and convert into string
static std::string s_recv (zmq::socket_t & socket) {

    zmq::message_t message;

    socket.recv(&message, ZMQ_NOBLOCK);

    return std::string(static_cast<char*>(message.data()), message.size());
}

//  Convert string to 0MQ string and send to socket
static bool s_send (zmq::socket_t & socket, const std::string & string) {

    zmq::message_t message(string.size());
    memcpy (message.data(), string.data(), string.size());

    bool rc = socket.send (message);
    return (rc);
}


std::atomic<bool> keep_running(true);

void interrupt_handler(int dummy) {
    keep_running.store(false);
}

bool received_terminate_message(void * subscriber) {

    bool return_val = false;

    char *address = s_recv_noblock(subscriber);
    char *contents = s_recv_noblock(subscriber);

    if (address && contents && strcmp(contents, "terminate") == 0) {
        return_val = true;
    }

    if (address) 
        free(address);
    if (contents)
        free(contents);

    return return_val;
}

// Perform a no-block message receive.  If no message is available return std::string("").
std::string receive_message(void * subscriber) {

    char *address = s_recv_noblock(subscriber);
    char *contents = s_recv_noblock(subscriber);

    std::string return_msg = "";

    if (address && contents) {
        return_msg = contents;
    }

    if (address)
        free(address);
    if (contents)
        free(contents);

    return return_msg;

}

// irods api client thread main routine
// this is the main loop that reads the change entries in memory and sends them to iRODS via the API.
void irods_api_client_main(std::shared_ptr<lustre_irods_connection> conn, const lustre_irods_connector_cfg_t *config_struct_ptr,
        change_map_t *change_map) {

    void *context = zmq_ctx_new();
    void *subscriber = zmq_socket(context, ZMQ_SUB);
    std::stringstream tcp_ss;
    tcp_ss << "tcp://localhost:" << config_struct_ptr->irods_client_recv_port;
    LOG(LOG_DBG, "client subscriber tcp_ss = %s\n", tcp_ss.str().c_str());
    zmq_connect(subscriber, tcp_ss.str().c_str());
    zmq_setsockopt(subscriber, ZMQ_SUBSCRIBE, "changetable_readers", 1);

    void *context2 = zmq_ctx_new();
    void *publisher = zmq_socket(context2, ZMQ_PUB);
    tcp_ss.str("");
    tcp_ss.clear();
    tcp_ss << "tcp://localhost:" << config_struct_ptr->changelog_reader_recv_port;
    LOG(LOG_DBG, "client publisher tcp_ss = %s\n", tcp_ss.str().c_str());
    zmq_connect(publisher, tcp_ss.str().c_str());

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

                // send message to changelog reader to continue reading changelog
                LOG(LOG_DBG, "sending continue message to changelog reader\n");
                s_sendmore(publisher, "changelog_reader");
                s_send(publisher, "continue");
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

                // send message to changelog reader to pause reading changelog
                LOG(LOG_DBG, "sending pause message to changelog_reader\n");
                s_sendmore(publisher, "changelog_reader");
                s_send(publisher, "pause");


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

            // see if there is a quit message, if so terminate
            if (received_terminate_message(subscriber)) {
                quit = true;
                break;
            }
        }

        // doing this in a loop so that we can catch a terminate more quickly
        // not sure how to interrupt the sleep with zmq
        for (unsigned int i = 0; i < error_backoff_timer; ++i) {
            if (quit || received_terminate_message(subscriber)) {
                quit = true;
                break;
            }

            sleep(1);
        }

        // see if there is a quit message, if so terminate
        if (quit) {
            LOG(LOG_DBG, "irods client received a terminate message\n");
            break;
        }
    }

    zmq_close(subscriber);
    zmq_ctx_destroy(context);

    LOG(LOG_DBG,"irods_client_exiting\n");
}


int main(int argc, char *argv[]) {

    const char *config_file = "lustre_irods_connector_config.json";
    char *log_file = nullptr;
    bool fatal_error_detected = false;

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

    LOG(LOG_DBG, "initializing change_map serialized database\n");
    if (initiate_change_map_serialization_database() < 0) {
        LOG(LOG_ERR, "failed to initialize serialization database\n");
        return 1;
    }

    // create the changemap in memory and read from serialized DB
    change_map_t change_map;

    LOG(LOG_DBG, "reading change_map from serialized database\n");
    if (deserialize_change_map_from_sqlite(&change_map) < 0) {
        LOG(LOG_ERR, "failed to deserialize change map on startup\n");
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

    // TODO change these ports to configuration
    
    // start a pub/sub publisher which is used to terminate threads
    void *context = zmq_ctx_new();
    void *publisher = zmq_socket(context, ZMQ_PUB);
    std::stringstream tcp_ss;
    tcp_ss << "tcp://*:" << config_struct.irods_client_recv_port;
    LOG(LOG_DBG, "main publisher tcp_ss = %s\n", tcp_ss.str().c_str());
    zmq_bind(publisher, tcp_ss.str().c_str());

    // start another pub/sub which is used for clients to send a stop reading
    // events message if iRODS is down
    void *context2 = zmq_ctx_new();
    void *subscriber = zmq_socket(context2, ZMQ_SUB);
    tcp_ss.str("");
    tcp_ss.clear();
    tcp_ss << "tcp://*:" << config_struct.changelog_reader_recv_port;
    LOG(LOG_DBG, "main subscriber tcp_ss = %s\n", tcp_ss.str().c_str());
    zmq_bind(subscriber, tcp_ss.str().c_str());
    zmq_setsockopt(subscriber, ZMQ_SUBSCRIBE, "changelog_reader", 1);

    // start the thread that sends changes to iRODS
    std::thread irods_api_client_thread(irods_api_client_main, conn, &config_struct, &change_map);

    rc = start_lcap_changelog(&config_struct);
    if (rc < 0) {
        LOG(LOG_ERR, "lcap_changelog_start: %s\n", zmq_strerror(-rc));
        fatal_error_detected = true;
    }

    bool pause_reading = false;

    while (!fatal_error_detected && keep_running.load()) {

        // check for a pause/continue message
        std::string msg = receive_message(subscriber);

        if (msg == "continue") 
            pause_reading = false;
        else if (msg == "pause")
            pause_reading = true;

        if (!pause_reading) {
            LOG(LOG_INFO,"changelog client polling changelog\n");
            poll_change_log_and_process(&config_struct, &change_map);
        } else {
            LOG(LOG_DBG, "in a paused state.  not reading changelog...\n");
        }
        sleep(config_struct.changelog_poll_interval_seconds);
    }

    // send message to threads to terminate
    LOG(LOG_DBG, "sending terminate message to client\n");
    s_sendmore(publisher, "changetable_readers");
    s_send(publisher, "terminate");

    irods_api_client_thread.join();


    LOG(LOG_DBG, "serializing change_map to database\n");
    if (serialize_change_map_to_sqlite(&change_map) < 0) {
        LOG(LOG_ERR, "failed to serialize change_map upon exit\n");
        fatal_error_detected = true;
    }

    zmq_close(publisher);
    zmq_ctx_destroy(context);

    rc = finish_lcap_changelog();
    if (rc) {
        LOG(LOG_ERR, "lcap_changelog_fini: %s\n", zmq_strerror(-rc));
        fatal_error_detected = true;
    } else {
        LOG(LOG_ERR, "finish_lcap_changelog exited normally\n");
    }

    LOG(LOG_DBG,"changelog client exiting\n");
    if (dbgstream != stdout) {
        fclose(dbgstream);
    }

    if (log_file) {
        free(log_file);
    }

    if (fatal_error_detected)
        return 1;

    return 0;
}
