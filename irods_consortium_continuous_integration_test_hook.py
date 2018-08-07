from __future__ import print_function

import optparse
import shutil
import irods_python_ci_utilities
import subprocess
import signal

# To run:
#  export PYTHONPATH=/root/irods_python_ci_utilities/irods_python_ci_utilities/
#  python irods_consortium_continuous_integration_test_hook.py --output_root_directory=<dir> --mds_ip_address=<ipaddr> --lustre_filesystem_name=<fsname>

def main():
    parser = optparse.OptionParser()
    parser.add_option('--output_root_directory')
    parser.add_option('--built_packages_root_directory')
    parser.add_option('--mds_ip_address')
    parser.add_option('--lustre_filesystem_name')
    options, _ = parser.parse_args()
    output_root_directory = options.output_root_directory
    built_packages_root_directory = options.built_packages_root_directory
    mds_ip_address = options.mds_ip_address
    lustre_filesystem_name = options.lustre_filesystem_name

    # start lcap
    lcap_process = subprocess.Popen(['/lcap/src/lcapd/lcapd',  '-c', '/etc/lcapd.conf'], shell=False)

    # remove existing mount and all lustre files
    irods_python_ci_utilities.subprocess_get_output(['rm', '-rf', '/lustreResc/lustre01/'])

    # mount the lustre file system with index 0
    irods_python_ci_utilities.subprocess_get_output(['mount', '-o', 'user_fid2path', '-t', 'lustre', '%s@tcp1:/%s' % (mds_ip_address, lustre_filesystem_name), '/lustreResc/lustre01'])

    # create a directory that goes to MTD 1
    irods_python_ci_utilities.subprocess_get_output(['lfs', 'mkdir', '-i', '1', '/lustreResc/lustre01/MDT0001dir'])
    irods_python_ci_utilities.subprocess_get_output(['chown', 'irods:irods', '/lustreResc/lustre01/MDT0001dir'])

    try:
        test_output_file = 'log/test_output.log'
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'su', '-', 'irods', '-c', 'python2 scripts/run_tests.py --xml_output --run_s=test_irods_tools_lustre 2>&1 | tee {0}; exit $PIPESTATUS'.format(test_output_file)], check_rc=True)
    finally:
        if output_root_directory:
            irods_python_ci_utilities.gather_files_satisfying_predicate('/var/lib/irods/log', output_root_directory, lambda x: True)
            shutil.copy('/var/lib/irods/log/test_output.log', output_root_directory)

        lcap_process.send_signal(signal.SIGINT)



if __name__ == '__main__':
    main()
