package org.apache.hadoop.hdftp;

import org.apache.ftpserver.ftplet.FileSystemFactory;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * Implemented FileSystemFactory to use HdfsFileSystemView and store DFS connection
 */
public class HdfsFileSystemFactory implements FileSystemFactory {

	private static final transient Logger log = LoggerFactory.getLogger(HdfsFileSystemFactory.class);
	
	private boolean createHome = true;
	
	private FileSystem hdfs;
	
	/**
	 * Constructor - set the hadoop admin and hdfs config file.
	 * @throws InterruptedException 
	 */
	public HdfsFileSystemFactory(final String hdfsSuperuser, final String hdfsResource)
			throws IOException, InterruptedException{
		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hdfsSuperuser);
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				HdfsConfiguration conf = new HdfsConfiguration();
				Path resPath = new Path(hdfsResource);
				conf.addResource(resPath);
				hdfs = DistributedFileSystem.get(conf);
				return null;
			}
	    });
	}
	
    /**
     * Should the home directories be created automatically
     * @return true if the file system will create the home directory if not available
     */
	public boolean isCreateHome() {
    	return createHome;
	}

    /**
     * Set if the home directories be created automatically
     * @param createHome true if the file system will create the home directory if not available
     */
	public void setCreateHome(boolean createHome) {
    	this.createHome = createHome;
	}
    
	public FileSystemView createFileSystemView(User user) throws FtpException {
		synchronized (user) {
			try {
	            // create home if does not exist
	            if (createHome) {
	                String homeDirStr = user.getHomeDirectory();
	                Path path = new Path(homeDirStr);
	                if (!hdfs.exists(path)) {
	                	if (!hdfs.mkdirs(path)) {
	                		log.warn("Cannot create user home :: " + homeDirStr);
	                		throw new FtpException("Cannot create user home :: " + homeDirStr);
	                	}
	                	hdfs.setOwner(path, user.getName(), ((HdFtpUser)user).getGroup());
	                }
	                if (hdfs.isFile(path)) {
	                    log.warn("Not a directory :: " + homeDirStr);
	                    throw new FtpException("Not a directory :: " + homeDirStr);
	                }
	            }
	            return new HdfsFileSystemView(hdfs, (HdFtpUser) user);
			} catch (IOException e) {
				log.error("Create user \"{}\" FileSystemView error.", user.getName(), e);
				throw new FtpException("Create user FileSystemView error.");
			}
		}
	}
}
