#ifndef __LUSTRE_CHANGE_TABLE_HPP
#define __LUSTRE_CHANGE_TABLE_HPP

#ifdef __cplusplus
extern "C" {
#endif

void lustre_close(const char *fidstr, const char *lustre_path);
void lustre_mkdir(const char *fidstr, const char *lustre_path);
void lustre_rmdir(const char *fidstr, const char *lustre_path);
void lustre_unlink(const char *fidstr, const char *lustre_path);
void lustre_rename(const char *fidstr, const char *old_lustre_path, char *new_lustre_path);
void lustre_create(const char *fidstr, const char *lustre_path);
void lustre_mtime(const char *fidstr, const char *lustre_path);
void lustre_trunc(const char *fidstr, const char *lustre_path);
void lustre_print_change_table();


#ifdef __cplusplus
}
#endif


#ifdef __cplusplus

#include <map>
#include <string>
#include <ctime>

namespace irods_lustre {

enum create_delete_event_type { NA, CREAT, UNLINK, RMDIR, MKDIR, RENAME };

std::string event_type_to_str(create_delete_event_type type); 

struct change_descriptor {
    std::string              lustre_path;
    create_delete_event_type last_event;
    time_t                   timestamp;
    bool                     oper_complete;
    std::string              original_path;
};



class lustre_change_table {
 public:
    static lustre_change_table *instance();
    void handle_close(const std::string& fidstr, const std::string& lustre_path);
    void handle_mkdir(const std::string& fidstr, const std::string& lustre_path);
    void handle_rmdir(const std::string& fidstr, const std::string& lustre_path);
    void handle_unlink(const std::string& fidstr, const std::string& lustre_path);
    void handle_rename(const std::string& fidstr, const std::string& old_path, std::string new_path);
    void handle_create(const std::string& fidstr, const std::string& lustre_path);
    void handle_mtime(const std::string& fidstr, const std::string& lustre_path);
    void handle_trunc(const std::string& fidstr, const std::string& lustre_path);
    void print_table();
 private:
    lustre_change_table() {};
    lustre_change_table(lustre_change_table const&); 
    lustre_change_table& operator=(lustre_change_table const&);
    std::map<std::string, change_descriptor> change_map;
};


}

#endif

#endif
