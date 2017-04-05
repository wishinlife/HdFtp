package org.apache.hadoop.hdftp;

import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.AuthorizationRequest;
import org.apache.ftpserver.ftplet.User;

import java.util.ArrayList;
import java.util.List;

/**
 * Implemented User to add group permissions
 */
public class HdFtpUser implements User {

	private String name = null;

	private String password = null;

	private String homeDir = null;

	private boolean isEnabled = true;
	
	private int maxIdleTimeSec = 0;
	
	private short fileReplication = 0;

	private String group = "ftpgroup";

	private List<Authority> authorities;
	
	/**
	 * Default constructor.
	 */
	public HdFtpUser(String name) {
		this.name = name;
	}

	/**
	 * Copy constructor.
	 */
	public HdFtpUser(User user) {
		HdFtpUser usr = (HdFtpUser) user;
		name = usr.getName();
		password = usr.getPassword();
		authorities = usr.getAuthorities();
		maxIdleTimeSec = usr.getMaxIdleTime();
		homeDir = usr.getHomeDirectory();
		isEnabled = usr.getEnabled();
		group = usr.getGroup();
		fileReplication = usr.getFileReplication();
	}
	
	/**
	 * Get the group of the user
	 */
	public String getGroup() {
		return group;
	}

	/**
	 * Set users' group
	 *
	 * @param group to set
	 */
	public void setGroup(String group) {
		this.group = group;
	}

	/**
	 * Get the user name.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Set user name.
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Get the user password.
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * Set user password.
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	public List<Authority> getAuthorities() {
		if (authorities != null) {
			return authorities;	//authorities.clone();
		} else {
			return null;
		}
	}

	public void setAuthorities(List<Authority> authorities) {
		if (authorities != null) {
			this.authorities = authorities;	//authorities.clone()
		} else {
			this.authorities = null;
		}
	}

	/**
	 * Get the maximum idle time in second.
	 */
	public int getMaxIdleTime() {
		return maxIdleTimeSec;
	}

	/**
	 * Set the maximum idle time in second.
	 */
	public void setMaxIdleTime(int idleSec) {
		maxIdleTimeSec = idleSec;
		if (maxIdleTimeSec < 0) {
			maxIdleTimeSec = 0;
		}
	}

	/**
	 * Get the user enable status.
	 */
	public boolean getEnabled() {
		return isEnabled;
	}

	/**
	 * Set the user enable status.
	 */
	public void setEnabled(boolean enabled) {
		this.isEnabled = enabled;
	}

	/**
	 * Get the user home directory.
	 */
	public String getHomeDirectory() {
		return homeDir;
	}

	/**
	 * Set the user home directory.
	 */
	public void setHomeDirectory(String homeDir) {
		if(homeDir.endsWith("/")){	  
			this.homeDir = homeDir.substring(0, homeDir.length() - 1);
		}else{
			this.homeDir = homeDir;    	
		}
	}

	/**
	 * String representation.
	 */
	public String toString() {
		return name;
	}

	/**
	 * {@inheritDoc}
	 */
	public AuthorizationRequest authorize(AuthorizationRequest request) {
		List<Authority> authorities = getAuthorities();
		for (int i = 0; i < authorities.size(); i++) {
			Authority authority = (Authority)authorities.get(i);
			if (authority.canAuthorize(request)) {
				request = authority.authorize(request);
				// authorization failed, request is null and return null
				// authorization success, return request
				return request;
			}
		}
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	public List<Authority> getAuthorities(Class<? extends Authority> clazz) {
		List<Authority> selected = new ArrayList<Authority>();
		for (int i = 0; i < authorities.size(); i++) {
			if (authorities.get(i).getClass().equals(clazz)) {
				selected.add(authorities.get(i));
			}
		}
		return selected;
	}

	public short getFileReplication() {
		return fileReplication;
	}

	public void setFileReplication(short fileReplication) {
		this.fileReplication = fileReplication;
	}
}
