package org.apache.hadoop.hdftp;

import org.apache.ftpserver.FtpServerConfigurationException;
import org.apache.ftpserver.ftplet.Authentication;
import org.apache.ftpserver.ftplet.AuthenticationFailedException;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.usermanager.AnonymousAuthentication;
import org.apache.ftpserver.usermanager.PasswordEncryptor;
import org.apache.ftpserver.usermanager.UsernamePasswordAuthentication;
import org.apache.ftpserver.usermanager.impl.ConcurrentLoginPermission;
import org.apache.ftpserver.usermanager.impl.ConcurrentLoginRequest;
import org.apache.ftpserver.usermanager.impl.TransferRatePermission;
import org.apache.ftpserver.usermanager.impl.TransferRateRequest;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.apache.ftpserver.usermanager.impl.WriteRequest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Extended AbstractUserManager to use  HdfsUser
 */
public class HdFtpPropertiesUserManager implements UserManager {

	private static final transient Logger LOG = LoggerFactory.getLogger(HdFtpPropertiesUserManager.class);

	private final static String PREFIX = "ftpserver.user.";

	//private static final String ATTR_LOGIN = "userid";

    private static final String ATTR_PASSWORD = "userpassword";

    private static final String ATTR_HOME = "homedirectory";

    private static final String ATTR_WRITE_PERM = "writepermission";

    private static final String ATTR_ENABLE = "enableflag";

    private static final String ATTR_MAX_IDLE_TIME = "idletime";

    private static final String ATTR_MAX_UPLOAD_RATE = "uploadrate";

    private static final String ATTR_MAX_DOWNLOAD_RATE = "downloadrate";

    private static final String ATTR_MAX_LOGIN_NUMBER = "maxloginnumber";

    private static final String ATTR_MAX_LOGIN_PER_IP = "maxloginperip";
    
    private static final String ATTR_FILE_REPLICATION = "filereplication";
    
	private static final String ATTR_GROUP = "group";

	private HashMap<String, HdFtpUser> usersCache;

	private File userDataFile = null;

	private PasswordEncryptor passwordEncryptor;
	
	private String adminName;

	/**
	 * Constructor - Initialize HdfsUsermanager
	 */
	public HdFtpPropertiesUserManager(String adminName, PasswordEncryptor passwordEncryptor, File userDataFile) {
		this.adminName = adminName;
		this.passwordEncryptor = passwordEncryptor;
		this.userDataFile = userDataFile;
		loadFromFile(userDataFile);
	}
	/**
	 * Retrieve the file used to load and store users
	 *
	 * @return The file
	 */
	public File getFile() {
		return userDataFile;
	}

	/**
	 * Set the file used to store and read users. 
	 *
	 * @param propFile A file containing users
	 */
	public void setFile(File propFile) {
		this.userDataFile = propFile;
		refresh();
	}

    /**
     * Retrieve the password encryptor used for this user manager
     * @return The password encryptor. Default to {@link Md5PasswordEncryptor}
     *  if no other has been provided
     */
	public PasswordEncryptor getPasswordEncryptor() {
		return passwordEncryptor;
	}

	/**
	 * Set the password encryptor to use for this user manager
	 *
	 * @param passwordEncryptor The password encryptor
	 */
	public void setPasswordEncryptor(PasswordEncryptor passwordEncryptor) {
		this.passwordEncryptor = passwordEncryptor;
	}

	/**
	 * Initialize user manager.
	 */
	private void loadFromFile(File userDataFile) {
		FileReader reader = null;
		BufferedReader br = null;
		try {
			if (userDataFile == null || !userDataFile.exists()) {
				LOG.error("User data file not found on file system : ", userDataFile.getPath());
				throw new FtpServerConfigurationException(
                         "User data file specified but could not be located on the file system : "
                                 + userDataFile.getPath());
			}
			
			reader = new FileReader(userDataFile);
			br = new BufferedReader(reader);
			String str = null;
			usersCache = new HashMap<String, HdFtpUser>();
			HashMap<String, String> userProps = new HashMap<String, String>();
			String username;
			HdFtpUser user;
			while((str = br.readLine()) != null) {
				str = str.trim();
				if (str.startsWith(PREFIX)) {
					String[] prop = str.split("\\.", 4);
					username = prop[2];
					if (!usersCache.containsKey(username)) {
						user = new HdFtpUser(username);
						usersCache.put(username, user);
					}
					prop = prop[3].split("=", 2);
					userProps.put(username + prop[0], prop[1]);
				}
			}
			br.close();
			reader.close();
			
			Iterator<Entry<String, HdFtpUser>> iter = usersCache.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, HdFtpUser> entry = iter.next();
				user = entry.getValue();
				username = user.getName();
				
				user.setPassword(userProps.get(username + ATTR_PASSWORD));
				user.setEnabled(getBoolean(userProps.get(username + ATTR_ENABLE), true));
				user.setHomeDirectory(userProps.get(username + ATTR_HOME));
				user.setMaxIdleTime(getInteger(userProps.get(username + ATTR_MAX_IDLE_TIME), 0));
				user.setGroup(userProps.get(username + ATTR_GROUP));
				user.setFileReplication((short) getInteger(userProps.get(username + ATTR_FILE_REPLICATION), 0));
				
				List<Authority> authorities = new ArrayList<Authority>();
				if (getBoolean(userProps.get(username + ATTR_WRITE_PERM), true))
					authorities.add(new WritePermission());
				int maxLogin = getInteger(userProps.get(username + ATTR_MAX_LOGIN_NUMBER), 0);
				int maxLoginPerIP = getInteger(userProps.get(username + ATTR_MAX_LOGIN_PER_IP), 0);
				authorities.add(new ConcurrentLoginPermission(maxLogin, maxLoginPerIP));
				int uploadRate = getInteger(userProps.get(username + ATTR_MAX_UPLOAD_RATE), 0);
				int downloadRate = getInteger(userProps.get(username + ATTR_MAX_DOWNLOAD_RATE), 0);
				authorities.add(new TransferRatePermission(downloadRate, uploadRate));
				user.setAuthorities(authorities);
			}
			userProps.clear();
		} catch (IOException e) {
			throw new FtpServerConfigurationException("Error loading user data file : " + userDataFile.getAbsolutePath(), e);
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {}
			}
			if (reader != null){
				try {
					reader.close();
				} catch (IOException e) {}
			}
		}
	}

	/**
	 * Save user data. Store the properties.
	 */
	public synchronized void save(User user) throws FtpException {
		usersCache.put(user.getName(), (HdFtpUser) user);
		saveUserData();
	}

	/**
	 * @throws FtpException
	 */
	private void saveUserData() throws FtpException {
		File dir = userDataFile.getAbsoluteFile().getParentFile();
		if (dir != null && !dir.exists() && !dir.mkdirs()) {
			String dirName = dir.getAbsolutePath();
			throw new FtpServerConfigurationException(
				      "Cannot create directory for user data file : " + dirName);
		}

		FileWriter fos = null;
		File newDataFile = new File(userDataFile.getAbsolutePath() + ".new");
		try {
			fos = new FileWriter(newDataFile);
			fos.write("Generated file - don't edit (please)\n");
			
			Iterator<Entry<String, HdFtpUser>> iter = usersCache.entrySet().iterator();
			HdFtpUser user;
			while (iter.hasNext()) {
				Entry<String, HdFtpUser> entry = iter.next();
				String username = entry.getKey();
				user = entry.getValue();
				
	            fos.write("\n# the user : " + username);
	            fos.write("\n" + PREFIX + username + "." + ATTR_PASSWORD + "=" + user.getPassword());
	            fos.write("\n" + PREFIX + username + "." + ATTR_HOME + "=" + user.getHomeDirectory());
	            fos.write("\n" + PREFIX + username + "." + ATTR_ENABLE + "=" + user.getEnabled());
	            
	            WriteRequest writeRequest = new WriteRequest();
	            writeRequest = (WriteRequest) user.authorize(writeRequest);
	            fos.write("\n" + PREFIX + username + "." + ATTR_WRITE_PERM + "=" + (writeRequest != null));
	            
	    		ConcurrentLoginRequest concurrentLoginRequest = new ConcurrentLoginRequest(0, 0);
	    		concurrentLoginRequest = (ConcurrentLoginRequest) user.authorize(concurrentLoginRequest);
	            fos.write("\n" + PREFIX + username + "." + ATTR_MAX_LOGIN_NUMBER + "=" + concurrentLoginRequest.getMaxConcurrentLogins());
	            fos.write("\n" + PREFIX + username + "." + ATTR_MAX_LOGIN_PER_IP + "=" + concurrentLoginRequest.getMaxConcurrentLoginsPerIP());
	            fos.write("\n" + PREFIX + username + "." + ATTR_MAX_IDLE_TIME + "=" + user.getMaxIdleTime());
	            
	    		TransferRateRequest transferRateRequest = new TransferRateRequest();
	    		transferRateRequest = (TransferRateRequest) user.authorize(transferRateRequest);
	            fos.write("\n" + PREFIX + username + "." + ATTR_MAX_UPLOAD_RATE + "=" + transferRateRequest.getMaxUploadRate());
	            fos.write("\n" + PREFIX + username + "." + ATTR_MAX_DOWNLOAD_RATE + "=" + transferRateRequest.getMaxDownloadRate());
	            fos.write("\n" + PREFIX + username + "." + ATTR_FILE_REPLICATION + "=" + user.getFileReplication());
	            fos.write("\n" + PREFIX + username + "." + ATTR_GROUP + "=" + user.getGroup());
			}
			fos.flush();
			fos.close();
			newDataFile.renameTo(userDataFile);
		} catch (IOException ex) {
			LOG.error("Failed saving user data", ex);
			throw new FtpException("Failed saving user data", ex);
		}
	}

	/**
	 * Delete an user. Removes all this user entries from the properties. After
	 * removing the corresponding from the properties, save the data.
	 */
	public synchronized void delete(String usrName) throws FtpException {
		usersCache.remove(usrName);
		saveUserData();
	}

	/**
	 * Get all user names.
	 */
	public synchronized String[] getAllUserNames() {
		return usersCache.keySet().toArray(new String[0]);
	}

	/**
	 * Load user data.
	 */
	public synchronized HdFtpUser getUserByName(String userName) {
		if (!usersCache.containsKey(userName)) {
			return null;
		}
		return usersCache.get(userName);
	}

	/**
	 * User existence check
	 */
	public synchronized boolean doesExist(String userName) {
		return usersCache.containsKey(userName);
	}

	/**
	 * User authenticate method
	 */
	public synchronized User authenticate(Authentication authentication)
				  throws AuthenticationFailedException {
		
		if (authentication instanceof UsernamePasswordAuthentication) {
			UsernamePasswordAuthentication upauth = (UsernamePasswordAuthentication) authentication;

			HdFtpUser user = getUserByName(upauth.getUsername());
			String password = upauth.getPassword();

			if (user == null) {
				throw new AuthenticationFailedException("Authentication failed");
			}
			if (password == null) {
				password = "";
			}

			if (passwordEncryptor.matches(password, user.getPassword())) {
				return user;
			} else {
				throw new AuthenticationFailedException("Authentication failed");
			}

		} else if (authentication instanceof AnonymousAuthentication) {
			if (doesExist("anonymous")) {
				return getUserByName("anonymous");
			} else {
				throw new AuthenticationFailedException("Authentication failed");
			}
		} else {
			throw new IllegalArgumentException(
				      "Authentication not supported by this user manager");
		}
	}

	/**
	 * Close the user manager - remove existing entries.
	 */
	public synchronized void dispose() {
		if (usersCache != null) {
			usersCache.clear();
			usersCache = null;
			userDataFile = null;
		}
	}

	@Override
	public String getAdminName() throws FtpException {
		return adminName;
	}

	@Override
	public boolean isAdmin(String login) throws FtpException {
		return adminName.equals(login);
	}
	
    /**
     * Reloads the contents of the users.properties file. This allows any manual modifications to the file to be recognised by the running server.
     */
    public void refresh() {
        synchronized (usersCache) {
            if (userDataFile != null) {
                LOG.debug("Refreshing user manager using file: " + userDataFile.getAbsolutePath());
                loadFromFile(userDataFile);
            }
        }
    }
    
    private int getInteger(String str, int defaultValue) {
    	try {
    		int value = Integer.parseInt(str);
    		if (value > 0)
    			return value;
    		else
    			return defaultValue;
    	} catch(Exception e) {
    		return defaultValue;
    	}
    }
    
    private boolean getBoolean(String str, boolean defaultValue) {
    	if (str != null && str != "")
    		return Boolean.parseBoolean(str);
    	else
    		return defaultValue;
    }
}
