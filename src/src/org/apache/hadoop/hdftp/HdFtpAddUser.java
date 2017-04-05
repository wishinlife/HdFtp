package org.apache.hadoop.hdftp;

import java.io.File;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.ftpserver.usermanager.Md5PasswordEncryptor;
import org.apache.ftpserver.usermanager.PasswordEncryptor;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.usermanager.impl.ConcurrentLoginPermission;
import org.apache.ftpserver.usermanager.impl.TransferRatePermission;
import org.apache.ftpserver.usermanager.impl.WritePermission;

public class HdFtpAddUser {	
	
	private static String CONF_USER = "conf/users.properties";

	public static void main(String[] args) throws Exception{
		//String hdftp_home = System.getProperty("java.class.path");
		String hdftp_home = HdFtpServer.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		hdftp_home = hdftp_home.substring(0, hdftp_home.lastIndexOf("/") + 1);
        
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            
            HdFtpPropertiesUserManager um = new HdFtpPropertiesUserManager("admin", new Md5PasswordEncryptor(), new File(hdftp_home + CONF_USER));

            System.out.println("Asking for details of the new user");
            
            System.out.println();
            String userName = askForString(in, "User name:", "User name is mandatory");
            if(userName == null) {
                return;
            }
            HdFtpUser user = new HdFtpUser(userName);
            
            String password = askForString(in, "Password:");
            PasswordEncryptor passwordEncryptor = new Md5PasswordEncryptor();
            password = passwordEncryptor.encrypt(password);
            user.setPassword(password);
            
            String groupName = askForString(in, "User group:", "User name is mandatory(ftpusers is default value )");
            if(groupName == null) {
            	groupName = "ftpusers";
            }
            user.setGroup(groupName);

            
            String home = askForString(in, "Home directory:", "Home directory is mandatory");
            if(home == null) {
                return;            
            }
            user.setHomeDirectory(home);
            
            user.setEnabled(askForBoolean(in, "Enabled (Y/N):"));

            user.setMaxIdleTime(askForInt(in, "Max idle time in seconds (0 for none):"));
            
            List<Authority> authorities = new ArrayList<Authority>();
            
            if(askForBoolean(in, "Write permission (Y/N):")) {
                authorities.add(new WritePermission());
            }

            int maxLogins = askForInt(in, "Maximum number of concurrent logins (0 for no restriction)");
            int maxLoginsPerIp = askForInt(in, "Maximum number of concurrent logins per IP (0 for no restriction)");
            
            authorities.add(new ConcurrentLoginPermission(maxLogins, maxLoginsPerIp));
            
            int downloadRate = askForInt(in, "Maximum download rate (0 for no restriction)");
            int uploadRate = askForInt(in, "Maximum upload rate (0 for no restriction)");
            
            authorities.add(new TransferRatePermission(downloadRate, uploadRate));
            
            user.setAuthorities(authorities);
            
            user.setFileReplication((short) askForInt(in, "File replication of user upload's (0 for server default):"));
            
            if (um.doesExist(userName)) {
            	if(!askForBoolean(in, "User \"" + userName + "\" already exist, are you overwrite(Y/N):")) {
            		System.exit(0);
            	}
            }
            um.save(user);
            System.out.println("User saved success.");

        } catch (Exception ex) {
            ex.printStackTrace();
        }

	}
	
    private static String askForString(BufferedReader in, String question) throws IOException {
        System.out.println(question);
        return in.readLine();
    }

    private static String askForString(BufferedReader in, String question, String mandatoryMessage) throws IOException {
        String s = askForString(in, question);
        if(isEmpty(s)) {
            System.err.println(mandatoryMessage);
            return null;
        } else {
            return s;
        }
    }
    
    private static int askForInt(BufferedReader in, String question) throws IOException {
        System.out.println(question);
        String intValue = in.readLine();
        return Integer.parseInt(intValue);
    }

    private static boolean askForBoolean(BufferedReader in, String question) throws IOException {
        System.out.println(question);
        String boolValue = in.readLine();
        return "Y".equalsIgnoreCase(boolValue);
    }
    
    private static boolean isEmpty(String s) {
        if(s == null) {
            return true;
        } else {
            return s.trim().length() == 0;
        }
    }
}
