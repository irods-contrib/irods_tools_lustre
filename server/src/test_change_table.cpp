#include <map>
#include <string>
#include <ctime>
#include <iostream>

#include "lustre_change_table.hpp"

const char *lustre_root_path = "/lustre01";
const char *register_path = "/tempZone/lustre";
const char *resource_name = "demoResc";
const int resource_id = 10014;


int main() {

    std::cout << resource_id << std::endl;

    //irods_lustre::lustre_change_table *t = irods_lustre::lustre_change_table::instance();
    lustre_create("f1", "d1", "file1.txt", "/lustre01/file1.txt");
    lustre_close("f1", "d1", "", "");
    lustre_trunc("f2", "d1", "", "");
    lustre_trunc("f3", "d1", "", "");
    lustre_close("f3", "d1", "", "");
    lustre_mtime("f2", "d1", "", "");
    lustre_mkdir("d2", "d0", "dir1", "/lustre01/dir1");
    //lustre_rmdir("d2", "d0", "", "" );
    lustre_mkdir("d3", "d0", "dir2", "/lustre01/dir2");
    lustre_rename("f1", "d0", "file1.txt", "/lustre01/file1a.txt", "/lustre01/file1.txt");
    lustre_rename("f4", "d0", "blah", "/lustre01/f4.txt", "/lustre01/blah");

    lustre_print_change_table();
    return 0;

}
