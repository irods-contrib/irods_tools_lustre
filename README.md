# Description

This is a connector to synchronize Lustre files with iRODS using the Lustre changelog and LCAPD.  This is currently in a prototype stage.  

# Prerequisites

- The computer where this will run is a Lustre client and has a mount point to Lustre.
  Note:  If the Lustre connector is to be run by a non-privileged account, mount the file system using the `-o user_fid2path`.  Example: `mount -o user_fid2path -t lustre mds1@tcp1:/lustre01 /lustreResc/lustre01`
- iRODS is installed (either locally or remotely) and is accessible from this computer.
- The account that will run this connector has the iRODS connection configured (via iinit). 
- The iRODS externals packages have been installed.  See https://packages.irods.org/.  
- ODBC Dev has been installed:
   sudo yum install unixODBC-devel
   sudo apt-get install unixodbc unixodbc-dev

# Build Instructions

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
sudo make install
```

3. Install capnproto as described in https://capnproto.org/install.html.  

Note:  To get this to build I had to change the following line in configure to add clang++ in the ac_prog list:

```
old:

for ac_prog in g++ c++ gpp aCC CC cxx cc++ cl.exe FCC KCC RCC xlC_r xlC

new:

for ac_prog in clang++ g++ c++ gpp aCC CC cxx cc++ cl.exe FCC KCC RCC xlC_r xlC
```

3. Clone the LCAP repository.

```
git clone https://github.com/cea-hpc/lcap
```

4. Build LCAP as described in the LCAP README.md file.

5.  Create an LCAPD configuration file in /etc/lcapd.config.  The following is a sample file.  Update the MDT name as necessary.

```
MDTDevice   lustre01-MDT0000
CLReader        cl1
Batch_Records   8192
Max_Buckets     256
LogType         stderr
```

6. Clone this repository. 

```
git clone https://github.com/irods-contrib/irods_tools_lustre
```

7.  Build the Lustre plugin for irods.

```
cd irods_tools_lustre/irods_lustre_plugin
mkdir bld
cd bld
cmake ..
make package
```
8.  Install the plugin.  Use the package version that applies to the database type you have for irods (postgres, mysql, or oracle).

Example for DEB and Postgres:

```
sudo dpkg -i irods-lustre-api-4.2.2-Linux-postgres.deb
```

Example for RPM and MySQL:

```
sudo rpm -i irods-lustre-api-4.2.2-Linux-mysql.rpm
```

9.  Build the Lustre-iRODS connector:

```
cd irods_tools_lustre/lustre_irods_connector
mkdir bld
cd bld
cmake ..
make
```

This will create an executable called lustre_irods_connector and a configuration file called lustre_irods_connector_config.json.  These can be copied to any desired location.

10.  Update lustre_irods_connector_config.json and set the following:

- mdtname - the name of the MDT in Lustre.
- lustre_root_path - the local mount point into Lustre
- irods_resource_name - the resource in iRODS
- resource_id
- log_level - one of LOG_FATAL, LOG_ERR, LOG_WARN, LOG_INFO, LOG_DBG
- irods_api_update_type - one of:
    - direct - iRODS plugin uses direct DB access for all changes
    - policy - iRODS plugin uses the iRODS API's for all changes
- register_map - an array of lustre_path to irods_path mappings
- thread_{n}_connection_paramters - irods_host and irods_port that thread n connects to.  If this is not defined the local iRODS environment (iinit) is used.

11.  Add the irods user on the MDS server with the same user ID and group ID as exists on the iRODS server.  Here is an example entry in /etc/passwd.

```
irods:x:498:498::/:/sbin/nologin
```

# Running the LCAPD daemon and running the connector.

1.  Update the changelog mask in Lustre so that we get all of the required events.  Perform the following on the MDT server.

```
sudo lctl set_param mdd.lustre01-MDT0000.changelog_mask="MARK CREAT MKDIR HLINK SLINK MKNOD UNLNK RMDIR RENME RNMTO OPEN LYOUT TRUNC SATTR XATTR HSM MTIME CTIME CLOSE"
```

2.  Start the LCAPD daemon.

```
/location/to/lcap/src/lcapd/lcapd -c /etc/lcapd.conf&

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

1.  Make sure each MDT server is defined in /etc/lcapd.conf prior to starting up lcapd.

2.  For any directory created with a command like "lfs mkdir -i 3 dir3", you must create that collection in iRODS and assign metadata on that collection to identify the directory's Lustre identifier.

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
path/to/lustre_irods_connector -c/path/to/config/file/lustre_irods_connector_config_MDT0000.json
path/to/lustre_irods_connector -c/path/to/config/file/lustre_irods_connector_config_MDT0001.json
```

