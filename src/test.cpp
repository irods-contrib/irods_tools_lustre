#include "irods_ops.h"
#include <stdio.h>
#include <string>
#include <iostream>
#include "rodsDef.h"

int main(int argc, char **argv) {
    std::string attr = "fidstr";
    std::string val = "0x200001b70:0x2d0:0x0";
    char irods_path[MAX_NAME_LEN];

    instantiate_irods_connection();

    if (find_irods_path_with_avu(attr.c_str(), val.c_str(), nullptr, false, irods_path) < 0) {
        std::cerr << "find_irods_path returned an error" << std::endl;
        return 1;
    }

    val = "0x200001b70:0x2cf:0x0";
   
    std::cout << "path: " << irods_path << std::endl;

    if (find_irods_path_with_avu(attr.c_str(), val.c_str(), nullptr, true, irods_path) < 0) {
        std::cerr << "find_irods_path returned an error" << std::endl;
        return 1;
    }

    std::cout << "path: " << irods_path << std::endl;

    disconnect_irods_connection();

}
