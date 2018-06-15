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
#include <sysexits.h>
#include <utility>

// local libraries
#include "irods_ops.hpp"
#include "lustre_change_table.hpp"
#include "config.hpp"
#include "lustre_irods_errors.hpp"
#include "changelog_poller.hpp"
#include "logging.hpp"

// irods libraries
#include "rodsDef.h"
#include "inout_structs.h"

// boost libraries
#include <boost/program_options.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>

#ifndef LPX64
#define LPX64   "%#llx"
#endif

#ifndef LCAP_CL_BLOCK
#define LCAP_CL_BLOCK   (0x01 << 1)
#endif

extern "C" {
  #include "lcap_cpp_wrapper.h"
}


namespace po = boost::program_options;

std::atomic<bool> keep_running(true);

void interrupt_handler(int dummy) {
    keep_running.store(false);
}

//  Sends string as 0MQ string, as multipart non-terminal 
static bool s_sendmore (zmq::socket_t& socket, const std::string& string) {

    zmq::message_t message(string.size());
    memcpy (message.data(), string.data(), string.size());

    bool bytes_sent;
    try {
        bytes_sent= socket.send (message, ZMQ_SNDMORE);
    } catch (const zmq::error_t& e) {
        bytes_sent = 0;
    }

    return (bytes_sent > 0);
}

//  Convert string to 0MQ string and send to socket
static bool s_send(zmq::socket_t& socket, const std::string& string) {

    zmq::message_t message(string.size());
    memcpy (message.data(), string.data(), string.size());

    size_t bytes_sent;
    try {
       bytes_sent = socket.send (message);
    } catch (const zmq::error_t& e) {
        bytes_sent = 0;
    }

    return (bytes_sent > 0);
}

//  Receive 0MQ string from socket and convert into string
static std::string s_recv_noblock(zmq::socket_t& socket) {

    zmq::message_t message;

    try {
        socket.recv(&message, ZMQ_NOBLOCK);
    } catch (const zmq::error_t& e) {
    }

    return std::string(static_cast<char*>(message.data()), message.size());
}

//  Receive 0MQ string from socket and ignore the return 
void s_recv_noblock_void(zmq::socket_t& socket) {

    zmq::message_t message;
    try {
        socket.recv(&message, ZMQ_NOBLOCK);
    } catch (const zmq::error_t& e) {
    }
}

bool received_terminate_message(zmq::socket_t& subscriber) {

    s_recv_noblock_void(subscriber);
    std::string contents = s_recv_noblock(subscriber);

    return contents == "terminate";

}


// Perform a no-block message receive.  If no message is available return std::string("").
std::string receive_message(zmq::socket_t& subscriber) {

    s_recv_noblock_void(subscriber);
    std::string contents = s_recv_noblock(subscriber);

    return contents;
}

int read_and_process_command_line_options(int argc, char *argv[], std::string& config_file) {
   
    po::options_description desc("Allowed options");
    try { 

        desc.add_options()
            ("help,h", "produce help message")
            ("config-file,c", po::value<std::string>(), "configuration file")
            ("log-file,l", po::value<std::string>(), "log file");
                                                                                                ;
        po::positional_options_description p;
        p.add("input-file", -1);
        
        po::variables_map vm;

        // read the command line arguments
        po::store(po::command_line_parser(argc, argv).options(desc).positional(p).run(), vm);
        po::notify(vm);

        if (vm.count("help")) {
            std::cout << "Usage:  lustre_irods_connector [options]" << std::endl;
            std::cout << desc << std::endl;
            return lustre_irods::QUIT;
        }

        if (vm.count("config-file")) {
            LOG(LOG_DBG,"setting configuration file to %s\n", vm["config-file"].as<std::string>().c_str());
            config_file = vm["config-file"].as<std::string>().c_str();
        }

        if (vm.count("log-file")) {
            std::string log_file = vm["log-file"].as<std::string>();
            dbgstream = fopen(log_file.c_str(), "a");
            if (nullptr == dbgstream) {
                dbgstream = stdout;
                LOG(LOG_ERR, "could not open log file %s... using stdout instead.\n", optarg);
            } else {
                LOG(LOG_DBG, "setting log file to %s\n", vm["log-file"].as<std::string>().c_str());
            }
        }
        return lustre_irods::SUCCESS;
    } catch (std::exception& e) {
         std::cerr << e.what() << std::endl;
         std::cerr << desc << std::endl;
         return lustre_irods::INVALID_OPERAND_ERROR;
    }

}

// this is the main changelog reader loop.  It reads changelogs, writes the records to an internal data structure, 
// and sends groups of changelog records to client updater threads.
void run_main_changelog_reader_loop(const lustre_irods_connector_cfg_t& config_struct, change_map_t& change_map, 
        lcap_cl_ctx_ptr& ctx, zmq::socket_t& publisher, zmq::socket_t& subscriber, zmq::socket_t& sender,
        std::set<std::string>& active_fidstr_list, unsigned long long& last_cr_index) {
    
    // create a vector holding the status of the client's connection to irods - true is up, false is down
    std::vector<bool> irods_api_client_connection_status(config_struct.irods_updater_thread_count, true);   
    unsigned int failed_connections_to_irods_count = 0;

    bool pause_reading = false;
    unsigned int sleep_period = config_struct.changelog_poll_interval_seconds;
    unsigned int max_number_of_changelog_records = config_struct.maximum_records_to_receive_from_lustre_changelog;

    while (keep_running.load()) {

        // check for a pause/continue message
        std::string msg;
        while ((msg = receive_message(subscriber)) != "") {
            LOG(LOG_DBG, "changelog client received message %s\n", msg.c_str());
        
            try {    
                if (boost::starts_with(msg, "pause:")) {
                    std::string thread_num_str = msg.substr(6);
                    unsigned int thread_num = boost::lexical_cast<unsigned int>(thread_num_str);
                    if (thread_num < irods_api_client_connection_status.size()) {
                        if (true == irods_api_client_connection_status[thread_num]) {
                            failed_connections_to_irods_count++;
                            irods_api_client_connection_status[thread_num] = false;
                        }
                    }

                } else if (boost::starts_with(msg, "continue:")) {
                    std::string thread_num_str = msg.substr(9);
                    unsigned int thread_num = boost::lexical_cast<unsigned int>(thread_num_str);
                    if (thread_num < irods_api_client_connection_status.size()) {
                        if (false == irods_api_client_connection_status[thread_num]) {
                            failed_connections_to_irods_count--;
                            irods_api_client_connection_status[thread_num] = true;
                        }
                    }

                } else {
                        LOG(LOG_ERR, "changelog client received message of unknown type.  Message: %s\n", msg.c_str());
                }
                LOG(LOG_DBG, "failed connection count is %d\n", failed_connections_to_irods_count);
            }  catch (boost::bad_lexical_cast) {
                // just ignore message if it isn't formatted properly
                LOG(LOG_ERR, "changelog client message was not formatted correctly.  Message: %s\n", msg.c_str());
            }

        }

        // if all the status of the irods connection for all threads is down, pause reading changelog until
        // one comes up
        pause_reading = (failed_connections_to_irods_count >= irods_api_client_connection_status.size()); 

        if (!pause_reading) {
            LOG(LOG_DBG,"changelog client polling changelog\n");
            poll_change_log_and_process(config_struct.mdtname, config_struct.lustre_root_path, config_struct.register_map, 
                    change_map, ctx, max_number_of_changelog_records - change_map.size(), last_cr_index);

            LOG(LOG_DBG, "change_map size: %lu\n", change_map.size());

            while (entries_ready_to_process(change_map)) {
                // get records ready to be processed into buf and buflen
                void *buf = nullptr;
                size_t buflen;
                int rc = write_change_table_to_capnproto_buf(&config_struct, buf, buflen,
                        change_map, active_fidstr_list);

                if (rc == lustre_irods::COLLISION_IN_FIDSTR) {
                    LOG(LOG_INFO, "----- Collision!  Breaking out -----\n");
                }

                // if we get a failure or we get a return code indicating that we must
                // wait on the completion of one fid to complete before continuing, 
                // then break out of this loop
                if (rc != lustre_irods::SUCCESS) {
                    free(buf);
                    break;
                }

                // send inp to irods updaters
                zmq::message_t message(buflen);
                memcpy(message.data(), buf, buflen);
                sender.send(message);

                free(buf);

            }

        } else {
            LOG(LOG_DBG, "in a paused state.  not reading changelog...\n");
        }

        LOG(LOG_DBG,"changelog client sleeping for %d seconds\n", sleep_period);
        sleep(sleep_period);
    }
}


// thread which reads the results from the irods updater threads and updates
// the change table in memory
void result_accumulator_main(const lustre_irods_connector_cfg_t *config_struct_ptr,
        change_map_t* change_map, std::set<std::string>* active_fidstr_list) {

    if (nullptr == change_map || nullptr == config_struct_ptr) {
        LOG(LOG_ERR, "result accumulator received a nullptr and is exiting.");
        return;
    }


    // set up broadcast subscriber for terminate messages 
    zmq::context_t context(1);  // 1 I/O thread
    zmq::socket_t subscriber(context, ZMQ_SUB);
    LOG(LOG_DBG, "result_accumulator subscriber conn_str = %s\n", config_struct_ptr->irods_client_broadcast_address.c_str());
    subscriber.connect(config_struct_ptr->irods_client_broadcast_address);
    std::string identity("changetable_readers");
    subscriber.setsockopt(ZMQ_SUBSCRIBE, identity.c_str(), identity.length());

    // set up receiver to receive results
    zmq::socket_t receiver(context,ZMQ_PULL);
    receiver.setsockopt(ZMQ_RCVTIMEO, config_struct_ptr->message_receive_timeout_msec);
    LOG(LOG_DBG, "result_accumulator receiver conn_str = %s\n", config_struct_ptr->result_accumulator_push_address.c_str());
    receiver.bind(config_struct_ptr->result_accumulator_push_address);
    receiver.connect(config_struct_ptr->result_accumulator_push_address);

    while (true) {
        zmq::message_t message;

        size_t bytes_received = 0;
        try {
            bytes_received = receiver.recv(&message);
        } catch (const zmq::error_t& e) {
            bytes_received = 0;
        }

        if (bytes_received > 0) {
            LOG(LOG_DBG, "accumulator received message of size: %lu.\n", message.size());
            unsigned char *buf = static_cast<unsigned char*>(message.data());
            std::string update_status;
            get_update_status_from_capnproto_buf(buf, message.size(), update_status);
            LOG(LOG_DBG, "update status received is %s\n", update_status.c_str());

            if (update_status == "FAIL") {
                add_capnproto_buffer_back_to_change_table(buf, message.size(), *change_map, *active_fidstr_list);
            } else {
                // remove all fidstr from active_fidstr_list 
                remove_fidstr_from_active_list(buf, message.size(), *active_fidstr_list);
            } 
            /*char response_flag[5];
            memcpy(response_flag, message.data(), 4);
            response_flag[4] = '\0';
            LOG(LOG_DBG, "response_flag is %s\n", response_flag);
            unsigned char *tmp= static_cast<unsigned char*>(message.data());
            unsigned char *response_buffer = tmp + 4;

            if (0 == strcmp(response_flag, "FAIL")) {
                add_capnproto_buffer_back_to_change_table(response_buffer, message.size() - 4, *change_map);
            }*/
        } 

        if ("terminate" == receive_message(subscriber)) {
            LOG(LOG_DBG, "result accumulator received a terminate message\n");
            break;
        }
    }
    LOG(LOG_DBG, "result accumulator exiting\n");


}

// irods api client thread main routine
// this is the main loop that reads the change entries in memory and sends them to iRODS via the API.
void irods_api_client_main(const lustre_irods_connector_cfg_t *config_struct_ptr,
        change_map_t* change_map, unsigned int thread_number) {

    if (nullptr == change_map || nullptr == config_struct_ptr) {
        LOG(LOG_ERR, "irods api client received a nullptr and is exiting.");
        return;
    }

    // set up broadcast subscriber for terminate messages
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
    receiver.setsockopt(ZMQ_RCVTIMEO, config_struct_ptr->message_receive_timeout_msec);
    LOG(LOG_DBG, "client (%u) push work conn_str = %s\n", thread_number, config_struct_ptr->changelog_reader_push_work_address.c_str());
    receiver.connect(config_struct_ptr->changelog_reader_push_work_address.c_str());

    // set up sender for sending update result status
    zmq::socket_t sender(context, ZMQ_PUSH);
    LOG(LOG_DBG, "client (%u) push results conn_str = %s\n", thread_number, config_struct_ptr->result_accumulator_push_address.c_str());
    sender.connect(config_struct_ptr->result_accumulator_push_address.c_str());

    bool quit = false;
    bool irods_error_detected = false;

    while (!quit) {

        zmq::message_t message;

        // initiate a connection object
        lustre_irods_connection conn(thread_number);

        size_t bytes_received = 0;
        try {
            bytes_received = receiver.recv(&message);
        } catch (const zmq::error_t& e) {
             bytes_received = 0;
        } 


        if (!irods_error_detected && bytes_received > 0) {

            irodsLustreApiInp_t inp {};
            inp.buf = static_cast<unsigned char*>(message.data());
            inp.buflen = message.size(); 

            LOG(LOG_INFO, "irods client (%u): received message of length %d\n", thread_number, inp.buflen);

            if (0 == conn.instantiate_irods_connection(config_struct_ptr, thread_number )) {

                // send to irods
                if (lustre_irods::IRODS_ERROR == conn.send_change_map_to_irods(&inp)) {
                    irods_error_detected = true;
                }
            } else {
                irods_error_detected = true;
            }

            if (irods_error_detected) {

                // irods was previous up but now is down

                // send message to changelog reader to pause reading changelog
                LOG(LOG_DBG, "irods client (%u): sending pause message to changelog_reader\n", thread_number);
                s_sendmore(publisher, "changelog_reader");
                std::string msg = str(boost::format("pause:%u") % thread_number);
                s_send(publisher, msg.c_str());

                // update the status to fail and send to accumulator
                unsigned char *buf = static_cast<unsigned char*>(message.data());
                size_t bufflen = message.size();
                set_update_status_in_capnproto_buf(buf, bufflen, "FAIL");
                zmq::message_t response_message(bufflen);
                memcpy(static_cast<char*>(response_message.data()), buf, bufflen);
                sender.send(response_message);
                free(buf);

            } else {
                // update the status to pass and send to accumulator
                unsigned char *buf = static_cast<unsigned char*>(message.data());
                size_t bufflen = message.size();
                set_update_status_in_capnproto_buf(buf, bufflen, "PASS");
                zmq::message_t response_message(bufflen);
                memcpy(static_cast<char*>(response_message.data()), buf, bufflen);
                sender.send(response_message);
                free(buf);

           }

        }  
        
        if (irods_error_detected) {
    
            // in a failure state, remain here until we have detected that iRODS is back up

            // try a connection in a loop until irods is back up. 
            do {

                // initiate a connection object
                lustre_irods_connection conn(thread_number);


                // sleep for sleep_period in a 1s loop so we can catch a terminate message
                for (unsigned int i = 0; i < config_struct_ptr->irods_client_connect_failure_retry_seconds; ++i) {
                    sleep(1);

                    // see if there is a quit message, if so terminate
                    if (received_terminate_message(subscriber)) {
                        LOG(LOG_DBG, "irods client (%u) received a terminate message\n", thread_number);
                        LOG(LOG_DBG,"irods client (%u) exiting\n", thread_number);
                        return;
                    }
                }

                // double sleep period
                //sleep_period = sleep_period << 1;

            } while (0 != conn.instantiate_irods_connection(config_struct_ptr, thread_number )); 
            
            // irods is back up, set status and send a message to the changelog reader
            
            irods_error_detected = false;
            LOG(LOG_DBG, "sending continue message to changelog reader\n");
            std::string msg = str(boost::format("continue:%u") % thread_number);
            s_sendmore(publisher, "changelog_reader");
            s_send(publisher, msg.c_str());
        }


        // see if there is a quit message, if so terminate
        if (received_terminate_message(subscriber)) {
             LOG(LOG_DBG, "irods client (%u) received a terminate message\n", thread_number);
             quit = true;
             break;
        }

    }

    LOG(LOG_DBG,"irods client (%u) exiting\n", thread_number);
}


int main(int argc, char *argv[]) {

    std::string config_file = "lustre_irods_connector_config.json";
    std::string log_file;
    bool fatal_error_detected = false;
    lcap_cl_ctx_ptr lcap_cl_ctx = nullptr;

    signal(SIGPIPE, SIG_IGN);
    
    struct sigaction sa;
    memset( &sa, 0, sizeof(sa) );
    sa.sa_handler = interrupt_handler;
    sigfillset(&sa.sa_mask);
    sigaction(SIGINT,&sa,NULL);

    int rc;

    rc = read_and_process_command_line_options(argc, argv, config_file);
    if (lustre_irods::QUIT == rc) {
        return EX_OK;
    } else if (lustre_irods::INVALID_OPERAND_ERROR == rc) {
        return  EX_USAGE;
    }

    lustre_irods_connector_cfg_t config_struct;
    rc = read_config_file(config_file, &config_struct);
    if (rc < 0) {
        return EX_CONFIG;
    }

    LOG(LOG_DBG, "initializing change_map serialized database\n");
    if (initiate_change_map_serialization_database() < 0) {
        LOG(LOG_ERR, "failed to initialize serialization database\n");
        return EX_SOFTWARE;
    }

    // create the changemap in memory and read from serialized DB
    change_map_t change_map;

    LOG(LOG_DBG, "reading change_map from serialized database\n");
    if (deserialize_change_map_from_sqlite(change_map) < 0) {
        LOG(LOG_ERR, "failed to deserialize change map on startup\n");
        return EX_SOFTWARE;
    }

    lustre_print_change_table(change_map);

    unsigned long long last_cr_index = 0;
    if (get_cr_index(last_cr_index) < 0) {
        LOG(LOG_ERR, "failed to get last cr_index.  continuing with 0 as index.\n");
    }

    LOG(LOG_DBG, "last_cr_index is %llu\n", last_cr_index);

    // connect to irods and get the resource id from the resource name 
    // uses irods environment for this initial connection
    { 
        lustre_irods_connection conn(0);

        rc = conn.instantiate_irods_connection(nullptr, 0); 
        if (rc < 0) {
            LOG(LOG_ERR, "instantiate_irods_connection failed.  exiting...\n");
            return EX_SOFTWARE;
        }

        // read the resource id from resource name
        rc = conn.populate_irods_resc_id(&config_struct); 
        if (rc < 0) {
            LOG(LOG_ERR, "populate_irods_resc_id returned an error\n");
            return EX_SOFTWARE;
        }
    }

    // create a std::set of fidstr which is used to pause sending updates to irods client updater threads
    // when a dependency is detected 
    std::set<std::string> active_fidstr_list;


    // start a pub/sub publisher which is used to terminate threads and to send irods up/down messages
    zmq::context_t context(1);
    zmq::socket_t publisher(context, ZMQ_PUB);
    LOG(LOG_DBG, "main publisher conn_str = %s\n", config_struct.irods_client_broadcast_address.c_str());
    publisher.bind(config_struct.irods_client_broadcast_address);

    // start another pub/sub which is used for clients to send a stop reading
    // events message if iRODS is down
    zmq::socket_t subscriber(context, ZMQ_SUB);
    LOG(LOG_DBG, "main subscriber conn_str = %s\n", config_struct.changelog_reader_broadcast_address.c_str());
    subscriber.bind(config_struct.changelog_reader_broadcast_address);
    std::string identity("changelog_reader");
    subscriber.setsockopt(ZMQ_SUBSCRIBE, identity.c_str(), identity.length());

    // start a PUSH notifier to send messages to the iRODS updater threads
    zmq::socket_t  sender(context, ZMQ_PUSH);
    sender.bind(config_struct.changelog_reader_push_work_address);

    // start accumulator thread which receives results back from iRODS updater threads
    std::thread accumulator_thread(result_accumulator_main, &config_struct, &change_map, &active_fidstr_list); 

    // create a vector of irods client updater threads 
    std::vector<std::thread> irods_api_client_thread_list;

    // start up the threads
    for (unsigned int i = 0; i < config_struct.irods_updater_thread_count; ++i) {
        std::thread t(irods_api_client_main, &config_struct, &change_map, i);
        irods_api_client_thread_list.push_back(std::move(t));
        //irods_api_client_connection_status.push_back(true);
    }

    LOG(LOG_DBG, "before start_lcap_changelog\n");
    rc = start_lcap_changelog(config_struct.mdtname, &lcap_cl_ctx, last_cr_index+1);
    LOG(LOG_DBG, "after start_lcap_changelog\n");
    if (rc < 0) {
        LOG(LOG_ERR, "lcap_changelog_start: %s\n", zmq_strerror(-rc));
        fatal_error_detected = true;
    }

    if (!fatal_error_detected) {
        run_main_changelog_reader_loop(config_struct, change_map, lcap_cl_ctx, publisher, subscriber, sender, active_fidstr_list, last_cr_index);
    }

    // send message to threads to terminate
    LOG(LOG_DBG, "sending terminate message to clients\n");
    s_sendmore(publisher, "changetable_readers");
    s_send(publisher, "terminate"); 

    //irods_api_client_thread.join();
    for (auto iter = irods_api_client_thread_list.begin(); iter != irods_api_client_thread_list.end(); ++iter) {
        iter->join();
    }

    accumulator_thread.join();

    LOG(LOG_DBG, "serializing change_map to database\n");
    if (serialize_change_map_to_sqlite(change_map) < 0) {
        LOG(LOG_ERR, "failed to serialize change_map upon exit\n");
        fatal_error_detected = true;
    }

    if (write_cr_index_to_sqlite(last_cr_index) < 0) {
        LOG(LOG_ERR, "failed to write cr_index to database upon exit\n");
        fatal_error_detected = true;
    }

    rc = finish_lcap_changelog(lcap_cl_ctx);
    if (rc) {
        LOG(LOG_ERR, "lcap_changelog_fini: %s\n", zmq_strerror(-rc));
        fatal_error_detected = true;
    } else {
        LOG(LOG_ERR, "finish_lcap_changelog exited normally\n");
    }

    LOG(LOG_DBG,"changelog client exiting\n");
    if (stdout != dbgstream) {
        fclose(dbgstream);
    }

    LOG(LOG_DBG,"this ran\n");
    if (fatal_error_detected) {
        return EX_SOFTWARE;
    }

    return EX_OK;
}
