#ifndef CHANGELOG_POLLER_H
#define CHANGELOG_POLLER_H

extern "C" {
  #include "lcap_cpp_wrapper.h"
}


int start_lcap_changelog(const std::string&, lcap_cl_ctx_ptr*);
int poll_change_log_and_process(const std::string&, const std::string&, change_map_t& change_map, lcap_cl_ctx_ptr);
int finish_lcap_changelog(lcap_cl_ctx_ptr);

#endif

