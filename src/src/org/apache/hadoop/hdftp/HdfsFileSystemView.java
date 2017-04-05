package org.apache.hadoop.hdftp;

import org.apache.ftpserver.ftplet.FtpFile;

import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implemented FileSystemView to use HdfsFileObject
 */
public class HdfsFileSystemView implements FileSystemView {

	private static final transient Logger log = LoggerFactory.getLogger(HdfsFileSystemView.class);

	// the first character will be '/' ,and the last character not with '/'
	// It is always with respect to the root directory.
	private String currDir = "/";

	private HdFtpUser user;
	
	private FileSystem hdfs;
	
	/**
	 * Constructor - set the user and dfs object.
	 */
	protected HdfsFileSystemView(FileSystem hdfs, HdFtpUser user) throws FtpException {
		if (user == null) {
			throw new IllegalArgumentException("User can not be null.");
		}
		if (user.getHomeDirectory() == null || user.getHomeDirectory() == "") {
			throw new IllegalArgumentException("User home directory can not be null.");
		}
		
		this.hdfs = hdfs;
		this.user = user;
	}

	/**
	 * Get the user home directory. It would be the file system root for the
	 * user.
	 */
	public FtpFile getHomeDirectory() throws FtpException {
		return new HdfsFtpFile(hdfs, currDir, user);
	}

	/**
	 * Change directory.
	 */
	public boolean changeWorkingDirectory(String dir) throws FtpException {
		String path = "/";
		log.debug("changeWorkingDirectory(): current directory: {}, select dir: {} .",  currDir, dir);
		if (dir.startsWith("/")) {
			path = dir;
			if (path.endsWith("/") && path != "/") {
				path = path.substring(0, path.length() - 1);
			}
		} else if (dir == "..") {
			int pos = currDir.lastIndexOf("/");
			if (pos > 0) {
				path = currDir.substring(0, pos);
			}
		} else if (dir != ".") {
			path = ((currDir == "/") ? "" : currDir) + "/" + dir;
		}
		
		HdfsFtpFile file = new HdfsFtpFile(hdfs, path, user);
		if (file.isDirectory() && file.isReadable()) {
			currDir = path;
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Is the file content random accessible?
	 */
	public boolean isRandomAccessible() {
		return true;
	}

	/**
	 * Dispose file system view - does nothing.
	 */
	public void dispose() {
		this.hdfs = null;
	}

	/**
	 * Get file.
	 */
	@Override
	public FtpFile getFile(String file) throws FtpException {
		log.debug("getFile(): file path: {} .", file);
	    String path;
	    if (file == "./")
	    	path = currDir;
	    else if (file.startsWith("/"))
	    	path = file;
	    else if (currDir.length() > 1)
	    	path = currDir + "/" + file;
	    else
	    	path = "/" + file;

	    return new HdfsFtpFile(hdfs, path, user);
	}
	
	/**
	 * Get the current directory.
	 */
	@Override
	public FtpFile getWorkingDirectory() throws FtpException {
		log.debug("getWorkingDirectory(): path: {} .", currDir);
		return new HdfsFtpFile(hdfs, currDir, user);
	}
}
