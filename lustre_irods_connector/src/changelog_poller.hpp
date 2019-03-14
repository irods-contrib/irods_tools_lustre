#ifndef CHANGELOG_POLLER_H
#define CHANGELOG_POLLER_H

extern "C" {
  #include "llapi_cpp_wrapper.h"
}

std::string get_fidstr_from_path(std::string path);

int start_changelog(const std::string&, cl_ctx_ptr*, unsigned long long start_cr_index);

int poll_change_log_and_process(const std::string& mdtname, 
        const std::string& changelog_reader, 
        const std::string& lustre_root_path, 
        const std::vector<std::pair<std::string, 
        std::string> >& register_map,
        change_map_t& change_map, 
        cl_ctx_ptr *ctx, 
        int max_records_to_retrieve, 
        unsigned long long& last_cr_index); 


int finish_changelog(void**);

#endif

