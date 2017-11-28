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
#include <stdio.h>
#include <iostream>

#include "irods_ops.hpp"
#include "lustre_change_table.hpp"
#include "config.hpp"
#include "lustre_irods_errors.hpp"

#include "rodsDef.h"
#include "inout_structs.h"
#include "logging.hpp"

#include <boost/program_options.hpp>
#include <boost/format.hpp>

#ifndef LPX64
#define LPX64   "%#llx"
#endif

#ifndef LCAP_CL_BLOCK
#define LCAP_CL_BLOCK   (0x01 << 1)
#endif

namespace po = boost::program_options;

struct lcap_cl_ctx;

extern "C" {
    int start_lcap_changelog(const char*, struct lcap_cl_ctx**);
    int poll_change_log_and_process(const char*, const char*, void *change_map, struct lcap_cl_ctx*);
    int finish_lcap_changelog(struct lcap_cl_ctx *);
}


std::atomic<bool> keep_running(true);

void interrupt_handler(int dummy) {
    keep_running.store(false);
}

//  Sends string as 0MQ string, as multipart non-terminal 
static bool s_sendmore (zmq::socket_t& socket, const std::string& string) {

    zmq::message_t message(string.size());
    memcpy (message.data(), string.data(), string.size());

    bool rc = socket.send (message, ZMQ_SNDMORE);
    return (rc);
}

//  Convert string to 0MQ string and send to socket
static bool s_send(zmq::socket_t& socket, const std::string& string) {

    zmq::message_t message(string.size());
    memcpy (message.data(), string.data(), string.size());

    bool rc = socket.send (message);
    return (rc);
}

//  Receive 0MQ string from socket and convert into string
static std::string s_recv_noblock(zmq::socket_t& socket) {

    zmq::message_t message;

    socket.recv(&message, ZMQ_NOBLOCK);

    return std::string(static_cast<char*>(message.data()), message.size());
}


// Perform a no-block message receive.  If no message is available return std::string("").
std::string receive_message(zmq::socket_t& subscriber) {

    std::string address = s_recv_noblock(subscriber);
    std::string contents = s_recv_noblock(subscriber);

    return contents;
}

// thread which reads the results from the irods updater threads and updates
// the change table in memory
void result_accumulator_main(const lustre_irods_connector_cfg_t *config_struct_ptr,
        change_map_t *change_map) {

    // set up broadcast subscriber for terminate messages 
    zmq::context_t context(1);  // 1 I/O thread
    zmq::socket_t subscriber(context, ZMQ_SUB);
    LOG(LOG_DBG, "result_accumulator subscriber conn_str = %s\n", config_struct_ptr->irods_client_broadcast_address.c_str());
    subscriber.connect(config_struct_ptr->irods_client_broadcast_address);
    std::string identity("changetable_readers");
    subscriber.setsockopt(ZMQ_SUBSCRIBE, identity.c_str(), identity.length());

    // set up receiver to receive results
    zmq::socket_t receiver(context,ZMQ_PULL);
    receiver.setsockopt(ZMQ_RCVTIMEO, 2000);
    LOG(LOG_DBG, "result_accumulator receiver conn_str = %s\n", config_struct_ptr->result_accumulator_push_address.c_str());
    receiver.bind(config_struct_ptr->result_accumulator_push_address);
    receiver.connect(config_struct_ptr->result_accumulator_push_address);

    while (true) {
        zmq::message_t message;
        if (receiver.recv(&message)) {
            LOG(LOG_DBG, "accumulator received message of size: %lu, message: %s\n", message.size(), message.data());
            char response_flag[5];
            memcpy(response_flag, message.data(), 4);
            response_flag[4] = '\0';
            LOG(LOG_DBG, "response_flag is %s\n", response_flag);
            unsigned char *tmp= static_cast<unsigned char*>(message.data());
            unsigned char *response_buffer = tmp + 4;

            if (strcmp(response_flag, "FAIL") == 0) {
                add_capnproto_buffer_back_to_change_table(response_buffer, message.size() - 4, change_map);
            }
        }

        if (receive_message(subscriber) == "terminate") {
            LOG(LOG_DBG, "result accumulator received a terminate message\n");
            break;
        }
    }
    LOG(LOG_DBG, "result accumulator exiting\n");


}

// irods api client thread main routine
// this is the main loop that reads the change entries in memory and sends them to iRODS via the API.
void irods_api_client_main(const lustre_irods_connector_cfg_t *config_struct_ptr,
        change_map_t *change_map, unsigned int thread_number) {

    // set up broadcast subscriber for terminate messages and to receive irods up/down messages
    zmq::context_t context(1);  // 1 I/O thread
    zmq::socket_t subscriber(context, ZMQ_SUB);
    LOG(LOG_DBG, "client (%u) subscriber conn_str = %s\n", thread_number, config_struct_ptr->irods_client_broadcast_address.c_str());
    subscriber.connect(config_struct_ptr->irods_client_broadcast_address.c_str());
    std::string identity("changetable_readers");
    subscriber.setsockopt(ZMQ_SUBSCRIBE, identity.c_str(), identity.length());

    // set up broadcast publisher for sending pause message to lustre log reader in case of irods failures
    //zmq::context_t context2(1);
    zmq::socket_t publisher(context, ZMQ_PUB);
    LOG(LOG_DBG, "client (%u) publisher conn_str = %s\n", thread_number, config_struct_ptr->changelog_reader_broadcast_address.c_str());
    publisher.connect(config_struct_ptr->changelog_reader_broadcast_address.c_str());

    // set up receiver for receiving update jobs 
    zmq::socket_t receiver(context, ZMQ_PULL);
    receiver.setsockopt(ZMQ_RCVTIMEO, 2000);
    LOG(LOG_DBG, "client (%u) push work conn_str = %s\n", thread_number, config_struct_ptr->changelog_reader_push_work_address.c_str());
    receiver.connect(config_struct_ptr->changelog_reader_push_work_address.c_str());

    // set up sender for sending update result status
    zmq::socket_t sender(context, ZMQ_PUSH);
    LOG(LOG_DBG, "client (%u) push results conn_str = %s\n", thread_number, config_struct_ptr->result_accumulator_push_address.c_str());
    sender.connect(config_struct_ptr->result_accumulator_push_address.c_str());

    // if we get an error sending data to irods, use a backoff_timer
    //unsigned int error_backoff_timer = config_struct_ptr->update_irods_interval_seconds;

    //bool irods_error_detected = false;
    bool quit = false;
    bool irods_error_detected = false;

    while (!quit) {

        // see if we have a work message
        zmq::message_t message;
        if (receiver.recv(&message)) {
            LOG(LOG_DBG, "irods client(%u): Received work message %s.\n", thread_number, (char*)message.data());


            irodsLustreApiInp_t inp {};
            inp.buf = static_cast<unsigned char*>(message.data());
            inp.buflen = message.size(); 

            LOG(LOG_DBG, "irods client (%u): received message of length %d\n", thread_number, inp.buflen);

            lustre_irods_connection conn(thread_number);

            if (conn.instantiate_irods_connection(config_struct_ptr, thread_number ) == 0) {

                // We previously had an error but it has cleared.  Send a message to changelog reader to
                // continue processing changelog.
                // TODO with multiple threads there may be an error but this flag isn't set on this thread
                if (irods_error_detected) {
                    irods_error_detected = false;
                    LOG(LOG_DBG, "sending continue message to changelog reader\n");
                    s_sendmore(publisher, "changelog_reader");
                    s_send(publisher, "continue");
                }

                // send to irods
                if (conn.send_change_map_to_irods(&inp) == lustre_irods::IRODS_ERROR) {
                    irods_error_detected = true;
                }
            } else {
                irods_error_detected = true;
            }

            // TODO work this out
            if (irods_error_detected) {

                // send message to changelog reader to pause reading changelog
                LOG(LOG_DBG, "irods client (%u): sending pause message to changelog_reader\n", thread_number);
                s_sendmore(publisher, "changelog_reader");
                s_send(publisher, "pause");

                // send a failure message to result accumulator.  Send "FAIL:" plus the original message
                zmq::message_t response_message(message.size() + 4);
                memcpy(static_cast<char*>(response_message.data()), "FAIL", 4);
                memcpy(static_cast<char*>(response_message.data())+4, message.data(), message.size());
                sender.send(response_message);

            } else {
                zmq::message_t response_message(message.size() + 4);
                memcpy(static_cast<char*>(response_message.data()), "PASS", 4);
                memcpy(static_cast<char*>(response_message.data())+4, message.data(), message.size());
                sender.send(response_message);

           }

        }

        std::string bcast_msg = receive_message(subscriber);
        if (bcast_msg == "terminate") {
            LOG(LOG_DBG, "irods client (%u) received a terminate message\n", thread_number);
            quit = true;
            break;
        } else if (bcast_msg == "pause") {
            LOG(LOG_DBG, "irods client (%u) received a pause message echoed from changlog reader\n", thread_number);
            irods_error_detected = true;
        } else if (bcast_msg == "continue") {
            LOG(LOG_DBG, "irods client (%u) received a pause message echoed from changlog reader\n", thread_number);
            irods_error_detected = false;
        }


    }

    LOG(LOG_DBG,"irods client (%u) exiting\n", thread_number);
}


int main(int argc, char *argv[]) {

    std::string config_file = "lustre_irods_connector_config.json";
    std::string log_file;
    bool fatal_error_detected = false;
    struct lcap_cl_ctx *ctx = nullptr;

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
            log_file = vm["log-file"].as<std::string>();
            dbgstream = fopen(log_file.c_str(), "a");
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

    // create a change_map for removed entries
    std::map<int, change_map_t> removed_entries;

    LOG(LOG_DBG, "reading change_map from serialized database\n");
    if (deserialize_change_map_from_sqlite(&change_map) < 0) {
        LOG(LOG_ERR, "failed to deserialize change map on startup\n");
        return 1;
    }

    // connect to irods and get the resource id from the resource name 
    { 
        lustre_irods_connection conn(0);

        rc = conn.instantiate_irods_connection(nullptr, 0); 
        if (rc < 0) {
            LOG(LOG_ERR, "instantiate_irods_connection failed.  exiting...\n");
            return 1;
        }

        // read the resource id from resource name
        rc = conn.populate_irods_resc_id(&config_struct); 
        if (rc < 0) {
            LOG(LOG_ERR, "populate_irods_resc_id returned an error\n");
            return 1;
        }
    }

    // start a pub/sub publisher which is used to terminate threads and to send irods up/down messages
    zmq::context_t context(1);
    zmq::socket_t publisher(context, ZMQ_PUB);
    LOG(LOG_DBG, "main publisher conn_str = %s\n", config_struct.irods_client_broadcast_address.c_str());
    publisher.bind(config_struct.irods_client_broadcast_address);

    // start another pub/sub which is used for clients to send a stop reading
    // events message if iRODS is down
    //zmq::context_t context2(1);
    zmq::socket_t subscriber(context, ZMQ_SUB);
    LOG(LOG_DBG, "main subscriber conn_str = %s\n", config_struct.changelog_reader_broadcast_address.c_str());
    subscriber.bind(config_struct.changelog_reader_broadcast_address);
    std::string identity("changelog_reader");
    subscriber.setsockopt(ZMQ_SUBSCRIBE, identity.c_str(), identity.length());

    // start a PUSH notifier to send messages to the iRODS updater threads
    zmq::socket_t  sender(context, ZMQ_PUSH);
    sender.bind(config_struct.changelog_reader_push_work_address);


    // start accumulator thread which receives results back from iRODS updater threads
    std::thread t(result_accumulator_main, &config_struct, &change_map); 

    // start threads that sends changes to iRODS
    std::vector<std::thread> irods_api_client_thread_list;
    for (unsigned int i = 0; i < config_struct.irods_updater_thread_count; ++i) {
        std::thread t(irods_api_client_main, &config_struct, &change_map, i);
        irods_api_client_thread_list.push_back(move(t));
    }

    rc = start_lcap_changelog(config_struct.mdtname.c_str(), &ctx);
    if (rc < 0) {
        LOG(LOG_ERR, "lcap_changelog_start: %s\n", zmq_strerror(-rc));
        fatal_error_detected = true;
    }

    bool pause_reading = false;
    unsigned int sleep_period = config_struct.changelog_poll_interval_seconds;

    while (!fatal_error_detected && keep_running.load()) {

        // check for a pause/continue message, read all from the queue but only act on last one
        std::string msg;
        std::string tmp;
        while ((tmp = receive_message(subscriber)) != "") {
            msg = tmp; 
        }

        if (msg != "") {
            LOG(LOG_DBG, "******** changelog client received message %s\n", msg.c_str());
        }

        if (msg == "continue") {
            LOG(LOG_INFO,"changelog client received a continue message, reseting sleep time\n");
            sleep_period = config_struct.changelog_poll_interval_seconds;
            pause_reading = false;

            // echo continue back to all clients
            LOG(LOG_DBG, "sending continue message to clients\n");
            s_sendmore(publisher, "changetable_readers");
            s_send(publisher, "continue");


        } else if (msg == "pause") {
            if (sleep_period <= config_struct.changelog_poll_interval_seconds << 1) {
                LOG(LOG_INFO,"changelog client received another pause message, increase sleep time\n");
                // if we keep getting pause messages, increase sleep time 
                sleep_period = sleep_period << 1;
            }
            // echo pause back to all clients
            LOG(LOG_DBG, "sending pause message to clients\n");
            s_sendmore(publisher, "changetable_readers");
            s_send(publisher, "pause");

           pause_reading = true;
        }

        if (!pause_reading) {
            LOG(LOG_INFO,"changelog client polling changelog\n");
            poll_change_log_and_process(config_struct.mdtname.c_str(), config_struct.lustre_root_path.c_str(), &change_map, ctx);
        } else {
            LOG(LOG_DBG, "in a paused state.  not reading changelog...\n");
        }


        // if we are paused, we need to query every cycle just to see if irods is back up
        // if we're paused this will only run once whether or not there are entries ready to be processed
        // if we're not paused - this will loop while entries are ready to process
        while (pause_reading || entries_ready_to_process(&change_map)) {
            // get records ready to be processed into buf and buflen
            void *buf = nullptr;
            int buflen;
            write_change_table_to_capnproto_buf(&config_struct, buf, buflen,
                            &change_map);

            // send inp to irods updaters
            zmq::message_t message(buflen);
            memcpy(message.data(), buf, buflen);
            sender.send(message);

            free(buf);

            if (pause_reading) {
                break;
            }
        }
        
        LOG(LOG_INFO,"changelog client sleeping for %d seconds\n", sleep_period);
        sleep(sleep_period);
    }

    // send message to threads to terminate
    LOG(LOG_DBG, "sending terminate message to clients\n");
    s_sendmore(publisher, "changetable_readers");
    s_send(publisher, "terminate");

    //irods_api_client_thread.join();
    for (auto iter = irods_api_client_thread_list.begin(); iter != irods_api_client_thread_list.end(); ++iter) {
        iter->join();
    }

    LOG(LOG_DBG, "serializing change_map to database\n");
    if (serialize_change_map_to_sqlite(&change_map) < 0) {
        LOG(LOG_ERR, "failed to serialize change_map upon exit\n");
        fatal_error_detected = true;
    }

    rc = finish_lcap_changelog(ctx);
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

    if (fatal_error_detected) {
        return 1;
    }

    return 0;
}
