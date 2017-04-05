General

the FTP server is base of hdfs-over-ftp-0.20.0, which works on a top of HDFS. It allows to connect to HDFS using any FTP client.
FTP server is configurable by hdftp.properties and users.properties. Also it allows to use secure connection over SSL and supports all HDFS permissions.

Installation

1. Download hdftp-2.7.1.tar.gz, unpack it.
2. Set users in users.properties. All passwords are md5 encrypted.
3. Set connection port, data-ports etc in hdftp.properties.
4. Set hdfs-uri, default replication and block size in hdfs-site.xml.
5. Start and stop server using hdftp.sh (start/stop)

Under linux you can mount ftp using curlftpfs:
sudo curlftpfs  -o allow_other ftp://user:pass@localhost:21 ftpfs

Frequently used commands
create new user:
     ./hdftp.sh adduser
start ftp server:
     ./hdftp.sh start
stop ftp server:
     ./hdftp.sh stop

History
version hdftp-2.7.1
1.hadoop version 2.7.1 is be supported

