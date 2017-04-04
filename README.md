# Description

This is a connector to synchronize Lustre files with iRODS using the Lustre changelog and LCAPD.  This is currently in a prototype stage.  Multithreading and batch updates to iRODS will be added in the near future.

# Prerequisites

- The computer where this will run is a Lustre client and has a mount point to Lustre.
- iRODS is installed (either locally or remotely) and is accessible from this computer.
- The account that will run this connector has the iRODS connection configured (via iinit). 
- The iRODS externals packages have been installed.  See https://packages.irods.org/.  

# Build Instructions

This application has a dependency LCAP.  

1. Build and install zeromq.

`
git clone https://github.com/zeromq/zeromq4-x
cd zeromq4-x 
./autogen.sh && ./configure && make
sudo make install
sudo ldconfig
cd ..
`

2. Clone the LCAP repository.

`git clone https://github.com/cea-hpc/lcap`

3. Build LCAP as described in the LCAP README.md file.

4.  Create an LCAPD configuration file in /etc/lcapd.config.  The following is a sample file.  Update the MDT name as necessary.

`
# Sample configuration file for LCAP

# MDT(s) to read records from
MDTDevice   lustre01-MDT0000

# ChangeLog reader identifier as provided by lctl during registration
CLReader        cl1

# How many records to send back per batch (bucket)
Batch_Records   8192

# How many buckets to keep in memory, per MDT
Max_Buckets     256

# Available loggers: stderr, syslog
LogType         stderr
`

4. Clone this repository.

`git clone https://github.com/irods-contrib/irods_tools_lustre`

5.  Update irods_tools_lustre/src/Makefile and change LCAP_ROOT to the location where the LCAP repository was cloned.

6.  Update the following global variables in irods_tools_lustre/src/connector.c:

- mdtname - the name of the MDT in Lustre.
- lustre_root_path - the local mount point into Lustre
- register_path - the path in iRODS where Lustre files will be registered 
- resource_name - the resource in iRODS

7.  Build the connector.

`
cd irods_tools_lustre/src
make connector
`

# Running the LCAPD daemon and running the connector.

1.  Update the changelog mask in Lustre so that we get all of the required events.  Perform the following on the MDT server.

`
sudo lctl set_param mdd.lustre01-MDT0000.changelog_mask="MARK CREAT MKDIR HLINK SLINK MKNOD UNLNK RMDIR RENME RNMTO OPEN LYOUT TRUNC SATTR XATTR HSM MTIME CTIME CLOSE"
`

2.  Start the LCAPD daemon.

`/location/to/lcap/src/lcapd/lcapd -c /etc/lcapd.conf&`

3.  Run the iRODS/Lustre connector.

`
export LD_LIBRARY_PATH=/opt/irods-externals/zeromq4-14.1.3-0/lib/
cd /path/to/irods_tools_lustre/src
./connector
`

4.  Make changes to Lustre and detect that these changes are picked up by the connector and files are registered/deregistered/etc. in iRODS.








