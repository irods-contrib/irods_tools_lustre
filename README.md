# Description

This is a connector to synchronize Lustre files with iRODS using the Lustre changelog.  This is currently in a prototype stage.  

# Prerequisites

- The computer where this will run is a Lustre client and has a mount point to Lustre.
  Note:  If the Lustre connector is to be run by a non-privileged account, mount the file system using the `-o user_fid2path`.  Example: `mount -o user_fid2path -t lustre mds1@tcp1:/lustre01 /lustreResc/lustre01`
- The account that will run this connector has the iRODS connection configured (via iinit). 
- The iRODS externals packages have been installed.  See https://packages.irods.org/.  
- The irods-dev package has been installed.
- Sqlite3 has been installed:
   - sudo apt-get install sqlite3 sqlite3-dev
   - sudo yum install libsqlite3x-devel
- ZMQ has been installed:
   - sudo apt-get install libzmq-dev
   - sudo yum install zeromq-devel
- ODBC Dev has been installed:
   - sudo yum install unixODBC-devel
   - sudo apt-get install unixodbc unixodbc-dev
- If running on RHEL, install the package rpm-build. 

# Build Instructions  

The connector and plugin must be built on a server that has iRODS installed.  Refer to [iRODS Docs](https://docs.irods.org/) for instructions on installing iRODS.  In addition the irods-dev (DEB) or irods-devel (RPM) package needs to be installed.

It is not required that iRODS be configured or running on the build server but the irods-server and an iRODS database plugin package are required to be installed.


1. Build and install zeromq.

```
git clone https://github.com/zeromq/zeromq4-x
cd zeromq4-x 
./autogen.sh && ./configure && make
sudo make install
sudo ldconfig
cd ..
```

2. Clone the jeayeson repository - https://github.com/jeaye/jeayeson.  Install the jeayeson header by running the following in the jeayeson directory:

```
sudo ./configure && sudo make install
```

3. Install capnproto as described in https://capnproto.org/install.html.  

Note:  To get this to build I had to change the following line in configure to add clang++ in the ac_prog list:

```
old:

for ac_prog in g++ c++ gpp aCC CC cxx cc++ cl.exe FCC KCC RCC xlC_r xlC

new:

export LD_LIBRARY_PATH=/opt/irods-externals/clang-runtime3.8-0/lib/:/opt/irods-externals/zeromq4-14.1.3-0/lib/
for ac_prog in clang++ g++ c++ gpp aCC CC cxx cc++ cl.exe FCC KCC RCC xlC_r xlC
```

4. Clone this repository. 

```
git clone https://github.com/irods-contrib/irods_tools_lustre
```

5.  Build the Lustre plugin for irods.   

```
export LD_LIBRARY_PATH=/opt/irods-externals/clang-runtime3.8-0/lib/:/opt/irods-externals/zeromq4-14.1.3-0/lib/
export PATH=/opt/irods-externals/cmake3.5.2-0/bin:$PATH
cd irods_tools_lustre/irods_lustre_plugin
mkdir bld
cd bld
cmake ..
make package
```
6.  Install the plugin on the iCAT server(s).  Use the package version that applies to the database type you have for irods (postgres, mysql, or oracle).

Example for DEB and Postgres:

```
sudo dpkg -i irods-lustre-api-4.2.2-Linux-postgres.deb
```

Example for RPM and MySQL:

```
sudo rpm -i irods-lustre-api-4.2.2-Linux-mysql.rpm
```

7.  Build the Lustre-iRODS connector:

```
export LD_LIBRARY_PATH=/opt/irods-externals/clang-runtime3.8-0/lib/:/opt/irods-externals/zeromq4-14.1.3-0/lib/
export PATH=/opt/irods-externals/cmake3.5.2-0/bin:$PATH
cd irods_tools_lustre/lustre_irods_connector
mkdir bld
cd bld
cmake ..
make
```

This will create an executable called lustre_irods_connector and a configuration file called lustre_irods_connector_config.json.  These can be copied to any desired location.

8.  Update lustre_irods_connector_config.json and set the following:

- mdtname - the name of the MDT in Lustre.
- changelog_reader - the changelog listener registerd on the MDS with the "lctl --device <mdt> changelog_register" command (example cl1)
- lustre_root_path - the local mount point into Lustre
- irods_resource_name - the resource in iRODS
- resource_id
- log_level - one of LOG_FATAL, LOG_ERR, LOG_WARN, LOG_INFO, LOG_DBG
- irods_api_update_type - one of:
    - direct - iRODS plugin uses direct DB access for all changes
    - policy - iRODS plugin uses the iRODS API's for all changes
- register_map - an array of lustre_path to irods_path mappings
    - The lustre_root_path needs to be in the register_map and must be the last entry in this map.
    - The entries must be ordered from more specific to less specific.  For example, "/mnt/dir1" should appear in the map before "/mnt"
- thread_{n}_connection_paramters - irods_host and irods_port that thread n connects to.  If this is not defined the local iRODS environment (iinit) is used.
- set_metadata_for_storage_tiering_time_violation (optional) - If set to "true" sets the metadata for update time on data objects to be compatible with the storage tiering plugin when using time violation policy.
- metadata_key_for_storage_tiering_time_violation (optional) - The metdata key used for the update time metdata on data objects.  The default is "irods::access_time".  This should be set to the same value that is configured in storage tiering.
  
Note that the last two settings are only valid when irods_api_update_type is "direct".  When policy is used the metadata is set by the rules as defined in the storage tiering policy.

9.  Add the irods user on the MDS server with the same user ID and group ID as exists on the iRODS server.  Here is an example entry in /etc/passwd.

```
irods:x:498:498::/:/sbin/nologin
```

# Running the connector.

1.  Update the changelog mask in Lustre so that we get all of the required events.  Perform the following for each MDT on the MDS server(s).

```
sudo lctl set_param mdd.lustre01-MDT0000.changelog_mask="CREAT CLOSE RENME UNLNK MKDIR RMDIR"
sudo lctl set_param mdd.lustre01-MDT0001.changelog_mask="CREAT CLOSE RENME UNLNK MKDIR RMDIR"
```

2.  On the MDS server(s), register a changelog listener for each MDT.

Example:

```
lctl --device lustre01-MDT0000 changelog_register
lctl --device lustre01-MDT0001 changelog_register
```

3.  Perform iinit to connect to the default iRODS host..

4.  Run the Lustre/iRODS connector.

```
/path/to/lustre_irods_connector
```

If desired you can send output to a log file:

```
/path/to/lustre_irods_connector -l connector.log
```

If the configuration file has been renamed or is not in the current location, use the -c switch to specify the configuration file:

```
/path/to/lustre_irods_connector -c /path/to/config/file/lustre_irods_connector_config.json
```

5.  Make changes to Lustre and detect that these changes are picked up by the connector and files are registered/deregistered/etc. in iRODS.


# Running Multiple Connectors for Clusters with Multiple MDT's.

If you have multiple MDT's, you can run multiple connectors with each assigned to a unique MDT. 

For any directory created with a command like "lfs mkdir -i 3 dir3", you must create that collection in iRODS and assign metadata on that collection to identify the directory's Lustre identifier.

Example setup:

```
$ lfs mkdir -i 3 dir3
$ lfs path2fid dir3
[0x280000400:0xd:0x0]
$ imkdir /tempZone/lustre01/dir3
$ imeta add -C /tempZone/lustre01/dir3 lustre_identifier 0x280000400:0xd:0x0
```

3.  Create separate lustre iRODS connector configuration files for each MDT with the mdtname paramter set to the MDT name.

4.  Start up the Lustre/iRODS connector multiple times specifying a unique configuration file each time.  

Example:

```
path/to/lustre_irods_connector -c /path/to/config/file/lustre_irods_connector_config_MDT0000.json&
path/to/lustre_irods_connector -c /path/to/config/file/lustre_irods_connector_config_MDT0001.json&
```

