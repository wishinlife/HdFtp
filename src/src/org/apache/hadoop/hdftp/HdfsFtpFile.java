package org.apache.hadoop.hdftp;

import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.usermanager.impl.WriteRequest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements all actions to HDFS
 */
public class HdfsFtpFile implements FtpFile {
	
	private static final transient Logger log = LoggerFactory.getLogger(HdfsFtpFile.class);

	private Path path;
	
	private String abspath;		//the full path from the base directory of the FileSystemView.
	
	private HdFtpUser user;
	
	private FileSystem hdfs;
	
	/**
	 * Constructs HdfsFileObject from path
	 *
	 * @param path path to represent object
	 * @param user accessor of the object
	 */
	public HdfsFtpFile(FileSystem hdfs, String path, HdFtpUser user) {
		this.path = new Path(user.getHomeDirectory() + path);
		this.abspath = path;
		this.user = user;
		this.hdfs = hdfs;
	}

	/**
	 * Get full name of the object
	 *
	 * @return full name of the object
	 */
	public String getAbsolutePath() {
		return this.abspath;
	}

	/**
	 * Get short name of the object
	 *
	 * @return short name of the object
	 */
	public String getName() {
		return abspath.substring(abspath.lastIndexOf("/") + 1);
	}

	/**
	 * HDFS has no hidden objects
	 *
	 * @return always false
	 */
	public boolean isHidden() {
		return false;
	}

	/**
	 * Checks if the object is a directory
	 *
	 * @return true if the object is a directory
	 */
	public boolean isDirectory() {
		try {
			log.debug("isDirectory(): {} .", path);
			FileStatus fs = hdfs.getFileStatus(path);
			return fs.isDirectory();
		} catch (IOException e) {
			log.error("isDirectory(): " + path + "  is not dir.", e);
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Get HDFS permissions
	 *
	 * @return HDFS permissions as a FsPermission instance
	 * @throws IOException if path doesn't exist so we get permissions of parent object in that case
	 */
	private FsPermission getPermissions() throws IOException {
		try {
			log.debug("getPermissions(): {} .", path);
			return hdfs.getFileStatus(path).getPermission();
		} catch (IOException e) {
			log.error("getPermissions(): " + path + " error.", e);
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Checks if the object is a file
	 *
	 * @return true if the object is a file
	 */
	public boolean isFile() {
		try {
			log.debug("isFile(): {} .", path);
			return hdfs.isFile(path);
		} catch (IOException e) {
			log.error("isFile(): " + path + " error.", e);
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Checks if the object does exist
	 *
	 * @return true if the object does exist
	 */
	public boolean doesExist() {
		try {
			log.debug("doesExist(): {}", path);
			return hdfs.exists(path);
		} catch (IOException e) {
			log.error("doesExist(): " + path +" error.", e);
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Checks if the user has a read permission on the object
	 *
	 * @return true if the user can read the object
	 */
	public boolean isReadable() {
		try {
			log.debug("isReadable(): {} .", path);
			FsPermission permissions = getPermissions();
			if (user.getName().equals(getOwnerName())) {
				if (permissions.toString().substring(0, 1).equals("r")) {
				  log.debug("isReadable(): " + path + " - " + " read allowed for user.");
				  return true;
				}
			} else if (user.getGroup() == getGroupName()) {
				if (permissions.toString().substring(3, 4).equals("r")) {
				  log.debug("isReadable(): " + path + " - " + " read allowed for group.");
				  return true;
				}
			} else if(permissions != null){
				if (permissions.toString().substring(6, 7).equals("r")) {
				  log.debug("isReadable(): " + path + " - " + " read allowed for others.");
				  return true;
				}
			}
			log.debug("isReadable(): " + path + " - " + " read denied.");
			return false;
		} catch (IOException e) {
			log.error("isReadable(): " + path + " error.", e);
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
			return false;
		}
	}

	private HdfsFtpFile getParent() {
		log.debug("getParent(): {} .", path);
		String parentS = abspath.substring(0, abspath.lastIndexOf("/") + 1);
		return new HdfsFtpFile(hdfs, parentS, user);
	}

	/**
	 * Checks if the user has a write permission on the object
	 *
	 * @return true if the user has write permission on the object
	 */
	public boolean isWritable() { 
		log.debug("isWriteable(): {} .", path);
		if(user.authorize(new WriteRequest()) == null) return false;
		try {
			if (!hdfs.exists(path)) return getParent().isWritable();
			FsPermission permissions = getPermissions();
			if (user.getName().equals(getOwnerName())) {
				if (permissions.toString().substring(1, 2).equals("w")) {
				  log.debug("isWriteable(): " + path + " - " + " write allowed for user.");
				  return true;
				}
			} else if (user.getGroup() == getGroupName()) {
				if (permissions.toString().substring(4, 5).equals("w")) {
				  log.debug("isWriteable(): " + path + " - " + " write allowed for group.");
				  return true;
				}
			} else if (permissions != null) {
				if (permissions.toString().substring(7, 8).equals("w")) {
				  log.debug("isWriteable(): " + path + " - " + " write allowed for others.");
				  return true;
				}
			}
			log.debug("isWriteable(): " + path + " - " + " write denied.");
			return false;
		} catch (IOException e) {
			log.error("isWriteable(): " + path + " error.", e);
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
			return false;
		}
	}

	/**
	 * Checks if the user has a delete permission on the object
	 *
	 * @return true if the user has delete permission on the object
	 */
	public boolean isRemovable() {
		return isWritable();
	}

	/**
	 * Get owner of the object
	 *
	 * @return owner of the object
	 */
	public String getOwnerName() {
		try {
			log.debug("getOwnerName(): {} .", path);
			FileStatus fs = hdfs.getFileStatus(path);
			return fs.getOwner();
		} catch (IOException e) {
			log.error("getOwnerName(): " + path + " error.", e);
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Get group of the object
	 *
	 * @return group of the object
	 */
	public String getGroupName() {
		try {
			log.debug("getGroupName(): {} .", path);
			FileStatus fs = hdfs.getFileStatus(path);
			return fs.getGroup();
		} catch (IOException e) {
			log.error("getGroupName(): " + path + " error.", e);
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Get link count
	 *
	 * @return 3 is for a directory and 1 is for a file
	 */
	public int getLinkCount() {
		return isDirectory() ? 3 : 1;
	}

	/**
	 * Get last modification date
	 *
	 * @return last modification date as a long
	 */
	public long getLastModified() {
		try {
			log.debug("getLastModified(): {} .", path);
			FileStatus fs = hdfs.getFileStatus(path);
			return fs.getModificationTime();
		} catch (IOException e) {
			log.error("getLastModified(): " + path + " error.", e);
			e.printStackTrace();
			return 0;
		}
	}

	/**
	 * Get a size of the object
	 *
	 * @return size of the object in bytes
	 */
	public long getSize() {
		try {
			log.debug("getSize(): {} .", path);
			FileStatus fs = hdfs.getFileStatus(path);
			return fs.getLen();
		} catch (IOException e) {
			log.error("getSize(): " + path + " error.", e);
			e.printStackTrace();
			return 0;
		}
	}

	/**
	 * Create a new directory from the object
	 *
	 * @return true if directory is created
	 */
	public boolean mkdir() {
		try {
			log.debug("mkdir(): {} .", path);
			if (hdfs.mkdirs(path)) {
				hdfs.setOwner(path, user.getName(), user.getGroup());
				return true;
			} else {
				return false;
			}
		} catch (IOException e) {
			log.error("mkdir(): " + path + " error.", e);
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Delete object from the HDFS file system
	 *
	 * @return true if the object is deleted
	 */
	public boolean delete() {
		log.debug("delete(): {} .", path);
		if(doesExist() == false) return false;
		try {
			return hdfs.delete(path, true);
		} catch (IOException e) {
			log.error("delete(): " + path + " error.", e);
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Move the object to another location
	 *
	 * @param fileObject location to move the object
	 * @return true if the object is moved successfully
	 */
	public boolean move(FtpFile fileObject) {
		try {
			log.debug("move(): srcPath: {}, destPath: {}{} .", path, user.getHomeDirectory(), fileObject.getAbsolutePath());
			return hdfs.rename(path, new Path(user.getHomeDirectory() + fileObject.getAbsolutePath()));
		} catch (IOException e) {
			log.error("move(): srcPath:" + path + ", destPath:" + user.getHomeDirectory() + fileObject.getAbsolutePath() + " error.", e);
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * List files of the directory
	 *
	 * @return List of files in the directory
	 */
	public List<FtpFile> listFiles() {
		if (!isReadable()) {
			log.debug("listFiles(): No read permission : {} .", path);
			return null;
		}
		log.debug("listFiles() : " + path);
		try {
			FileStatus fileStats[] = hdfs.listStatus(path);

			List<FtpFile> fileObjects = new ArrayList<FtpFile>();
			for (int i = 0; i < fileStats.length; i++) {
				String path = fileStats[i].getPath().toString();
				String userHome = user.getHomeDirectory();
				if(path.startsWith("hdfs://"))
					path = path.substring(path.indexOf("/", 7));
				path = path.substring(userHome.length());
				fileObjects.add(new HdfsFtpFile(hdfs, path, user));
			}
			return fileObjects;
		} catch (IOException e) {
			log.error("listFiles(): " + path, e);
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Creates output stream to write to the object
	 *
	 * @param offset is not used here
	 * @return OutputStream
	 * @throws IOException
	 */
	public OutputStream createOutputStream(final long offset) throws IOException {
		log.debug("createOutputStream(): {} .", path);
		// permission check
		if (!isWritable()) {
			throw new IOException("No write permission : " + path);
		}

		try {
			FSDataOutputStream os;
			if (user.getFileReplication() == (short) 0)
				os = hdfs.create(path);
			else
				os = hdfs.create(path, user.getFileReplication());
			hdfs.setOwner(path, user.getName(), user.getGroup());
			return os;
		} catch (IOException e) {
			log.error("createOutputStream(): " + path + " error.", e);
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Creates input stream to read from the object
	 *
	 * @param offset is not used here
	 * @return OutputStream
	 * @throws IOException
	 */
	public InputStream createInputStream(final long offset) throws IOException {
		log.debug("createInputStream(): {} .", path);
		// permission check
		if (!isReadable()) {
			throw new IOException("No read permission : " + path);
		}
		try {
			return hdfs.open(path);
		} catch (IOException e) {
			log.error("createInputStream(): " + path + " error.", e);
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Set file last modify time
	 */
	@Override
	public boolean setLastModified(long mtime) {
		try {
			log.debug("setLastModified(): {} .", path);
			//long atime = hdfs.getFileStatus(path).getAccessTime();
			hdfs.setTimes(path, mtime, mtime);
			return true;
		} catch (IOException e) {
			log.error("setLastModified(): " + path, e);
			e.printStackTrace();
			return false;
		}
	}
}
