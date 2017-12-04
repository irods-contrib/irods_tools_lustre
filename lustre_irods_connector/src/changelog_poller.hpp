#ifndef __CHANGELOG_POLLER_H
#define __CHANGELOG_POLLER_H

int start_lcap_changelog(const std::string&, void**);
int poll_change_log_and_process(const std::string&, const std::string&, change_map_t& change_map, void*);
int finish_lcap_changelog(void*);

#endif

