import itertools
import re
import sys
import json
import subprocess
import time
import signal
import glob
import shutil
import os

if sys.version_info >= (2, 7):
    import unittest
else:
    import unittest2 as unittest

from .resource_suite import ResourceBase

from .. import test
from . import settings
from . import session
from .. import lib
from ..configuration import IrodsConfig

# TODO 
# 1) install lcapd
# 2) install lustre?
# 3) run lcapd
# 4) create /lustreResc/lustre
# 5) mount lustre to /lustreResc/lustre01
# 6) lfs mkdir -i 3 /lustreResc/lustre01/OST0001dir
# 7) add lustre_identifier metadata to /lustreResc/lustre01/OST0001dir
# 7) workaround for sudo

class Test_Lustre(unittest.TestCase):

    def setUp(self):
        super(Test_Lustre, self).setUp()
        self.clean_up_lustre_files()
        self.admin= session.make_session_for_existing_admin()
        self.connector_list = []
        hostname = lib.get_hostname()
        #self.admin.assert_icommand("iadmin mkresc lustreResc unixfilesystem %s:/lustreResc" % hostname, 'STDOUT_SINGLELINE', 'unixfilesystem')
        super(Test_Lustre, self).setUp()

    def tearDown(self):
        self.clean_up_lustre_files()
        #self.admin.assert_icommand("iadmin rmresc lustreResc")

        # in case this wasn't cleaned up
        for connector in self.connector_list:
            connector.send_signal(signal.SIGINT)

        time.sleep(5)

        super(Test_Lustre, self).tearDown()

    @staticmethod
    def clean_up_lustre_files():
        for path in glob.glob("/lustreResc/lustre01/*"):
            # don't remove the subdirectory that is assigned to MDT3
            if path == '/lustreResc/lustre01/MDT0001dir':
                pass
            elif os.path.isfile(path):
                os.remove(path)
            elif os.path.isdir(path):
                shutil.rmtree(path)

        for path in glob.glob("/lustreResc/lustre01/MDT0001dir/*"):
            if os.path.isfile(path):
                os.remove(path)
            elif os.path.isdir(path):
                shutil.rmtree(path)


    @staticmethod
    def write_to_file(filename, contents):
        with open(filename, 'wt') as f:
            f.write(contents)

    @staticmethod
    def append_to_file_with_newline(filename, contents):
        with open(filename, 'a') as f:
            f.write('\n')
            f.write(contents)

    @staticmethod
    def setup_configuration_file(filename, mode, mdtname, begin_port):
        register_map1 = {
            'lustre_path': '/lustreResc/lustre01/home',
            'irods_register_path': '/tempZone/home'
        }

        register_map2 = {
            'lustre_path': '/lustreResc/lustre01/a',
            'irods_register_path': '/tempZone/a'
        }

        register_map3 = {
            'lustre_path': '/lustreResc/lustre01',
            'irods_register_path': '/tempZone/lustre01'
        }


        register_map_list = [register_map1, register_map2, register_map3]


        lustre_config = {
            "mdtname": mdtname,
            "lustre_root_path": "/lustreResc/lustre01",
            "irods_resource_name": "lustreResc",
            "irods_api_update_type": mode,
            "log_level": "LOG_ERR",
            "changelog_poll_interval_seconds": 1,
            "irods_client_connect_failure_retry_seconds": 30,
            "irods_client_broadcast_address": "tcp://127.0.0.1:%d" % begin_port,
            "changelog_reader_broadcast_address": "tcp://127.0.0.1: %d" % (begin_port + 1),
            "changelog_reader_push_work_address": "tcp://127.0.0.1:%d" % (begin_port + 2),
            "result_accumulator_push_address": "tcp://127.0.0.1:%d" % (begin_port + 3),
            "irods_updater_thread_count": 5,
            "maximum_records_per_update_to_irods": 200,
            "maximum_records_per_sql_command": 1,
            "maximum_records_to_receive_from_lustre_changelog": 500,
            "message_receive_timeout_msec": 2000,
            "register_map": register_map_list

        }

        with open(filename, 'wt') as f:
            json.dump(lustre_config, f, indent=4, ensure_ascii=False)


    def perform_standard_tests(self):

        self.write_to_file('/lustreResc/lustre01/file1', 'contents of file1') 
        time.sleep(3)
        self.admin.assert_icommand(['ils', '/tempZone/lustre01/file1'], 'STDOUT_MULTILINE', ['  /tempZone/lustre01/file1'])
        self.admin.assert_icommand(['iget', '/tempZone/lustre01/file1', '-'], 'STDOUT_MULTILINE', ['contents of file1'])

        self.append_to_file_with_newline('/lustreResc/lustre01/file1', 'line2')
        time.sleep(3)
        self.admin.assert_icommand(['iget', '/tempZone/lustre01/file1', '-'], 'STDOUT_MULTILINE', ['contents of file1', 'line2'])

        lib.execute_command(['mv',  '/lustreResc/lustre01/file1', '/lustreResc/lustre01/file2'])
        time.sleep(3)
        self.admin.assert_icommand(['ils', '/tempZone/lustre01/file2'], 'STDOUT_MULTILINE', ['  /tempZone/lustre01/file2'])

        lib.execute_command(['mkdir',  '/lustreResc/lustre01/dir1'])
        time.sleep(3)
        self.admin.assert_icommand(['ils', '/tempZone/lustre01/dir1'], 'STDOUT_MULTILINE', ['/tempZone/lustre01/dir1:'])

        lib.execute_command(['mv',  '/lustreResc/lustre01/file2', '/lustreResc/lustre01/dir1'])
        time.sleep(3)
        self.admin.assert_icommand(['ils', '/tempZone/lustre01/dir1'], 'STDOUT_MULTILINE', ['/tempZone/lustre01/dir1:', '  file2'])

        lib.execute_command(['mv',  '/lustreResc/lustre01/dir1', '/lustreResc/lustre01/dir2'])
        time.sleep(3)
        self.admin.assert_icommand(['ils', '/tempZone/lustre01/dir2'], 'STDOUT_MULTILINE', ['/tempZone/lustre01/dir2:', '  file2'])
        self.admin.assert_icommand(['ils', '-L', '/tempZone/lustre01/dir2/file2'], 'STDOUT_SINGLELINE', '        generic    /lustreResc/lustre01/dir2/file2')

        lib.execute_command(['rm',  '-rf', '/lustreResc/lustre01/dir2'])
        time.sleep(3)
        self.admin.assert_icommand(['ils', '/tempZone/lustre01/dir2'], 'STDERR_SINGLELINE', 'does not exist')

    def perform_multi_mdt_tests(self):

        self.write_to_file('/lustreResc/lustre01/file1', 'contents of file1') 
        time.sleep(3)
        self.admin.assert_icommand(['ils', '/tempZone/lustre01/file1'], 'STDOUT_MULTILINE', ['  /tempZone/lustre01/file1'])
        self.admin.assert_icommand(['iget', '/tempZone/lustre01/file1', '-'], 'STDOUT_MULTILINE', ['contents of file1'])

        lib.execute_command(['mv',  '/lustreResc/lustre01/file1', '/lustreResc/lustre01/MDT0001dir/file1'])
        time.sleep(3)
        self.admin.assert_icommand(['ils', '/tempZone/lustre01/file1'], 'STDERR_SINGLELINE', 'does not exist')
        self.admin.assert_icommand(['ils', '/tempZone/lustre01/MDT0001dir/file1'], 'STDOUT_MULTILINE', ['  /tempZone/lustre01/MDT0001dir/file1'])

    def test_lustre_direct(self):
        config_file = '/etc/irods/MDT0000.json'
        self.setup_configuration_file(config_file, 'direct', 'lustre01-MDT0000', 5555)
        self.connector_list.append(subprocess.Popen(['/bin/lustre_irods_connector',  '-c', config_file], shell=False))
        time.sleep(10)
        self.perform_standard_tests()

    def test_lustre_policy(self):
        config_file = '/etc/irods/MDT0000.json'
        self.setup_configuration_file(config_file, 'policy', 'lustre01-MDT0000', 5555)
        self.connector_list.append(subprocess.Popen(['/bin/lustre_irods_connector',  '-c', config_file], shell=False))
        time.sleep(10)
        self.perform_standard_tests()
        #self.connector_process.send_signal(signal.SIGINT) 

    def test_lustre_multi_mdt(self):
        config_file1 = '/etc/irods/MDT0000.json'
        self.setup_configuration_file(config_file1, 'direct', 'lustre01-MDT0000', 5555)
        self.connector_list.append(subprocess.Popen(['/bin/lustre_irods_connector',  '-c', config_file1], shell=False))
        config_file2 = '/etc/irods/MDT0001.json'
        self.setup_configuration_file(config_file2, 'direct', 'lustre01-MDT0001', 5565)
        self.connector_list.append(subprocess.Popen(['/bin/lustre_irods_connector',  '-c', config_file2], shell=False))
        time.sleep(10)
        self.perform_multi_mdt_tests()
