# Description

This is a connector to synchronize Lustre files with iRODS using the Lustre changelog and LCAPD.  This is currently in a prototype stage.  

# Prerequisites

- The computer where this will run is a Lustre client and has a mount point to Lustre.
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
- register_path - the path in iRODS where Lustre files will be registered 
- resource_name - the resource in iRODS
- resource_id
- log_level - one of LOG_FATAL, LOG_ERR, LOG_WARN, LOG_INFO, LOG_DBG
- irods_api_update_type - one of "direct" (if desire direct DB access in the iRODS Lustre plugin) or "policy" (if desire everything to go through the iRODS API's).

# Running the LCAPD daemon and running the connector.

1.  Update the changelog mask in Lustre so that we get all of the required events.  Perform the following on the MDT server.

```
sudo lctl set_param mdd.lustre01-MDT0000.changelog_mask="MARK CREAT MKDIR HLINK SLINK MKNOD UNLNK RMDIR RENME RNMTO OPEN LYOUT TRUNC SATTR XATTR HSM MTIME CTIME CLOSE"
```

2.  Start the LCAPD daemon.

```
/location/to/lcap/src/lcapd/lcapd -c /etc/lcapd.conf&
```

3.  Run the iRODS/Lustre connector.

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

4.  Make changes to Lustre and detect that these changes are picked up by the connector and files are registered/deregistered/etc. in iRODS.

