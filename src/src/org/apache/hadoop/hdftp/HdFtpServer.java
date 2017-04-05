package org.apache.hadoop.hdftp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.ftpserver.DataConnectionConfigurationFactory;
import org.apache.ftpserver.FtpServerFactory;
//import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.FtpServer;
import org.apache.log4j.PropertyConfigurator;
import org.apache.ftpserver.ConnectionConfigFactory;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.Md5PasswordEncryptor;
import org.apache.ftpserver.ssl.SslConfigurationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Start-up class of FTP server
 */
public class HdFtpServer {

	private static final transient Logger log = LoggerFactory.getLogger(HdFtpServer.class);

	private static String CONF_FILE =  "conf/hdftp.properties";
	private static String CONF_USER = "conf/users.properties";
	private static String CONF_LOG = "conf/log4j.properties";
	private static String SSL_KEY_FILE = "conf/ftpserver.jks";
	private static String HDFS_SITE = "conf/hdfs-site.xml";

	private static int port = 0;
	private static int sslPort = 0;
	private static String passivePorts = null;
	private static String sslPassivePorts = null;
	private static String sslPassword = null;
	private static String hdfsSuperuser = null;

	private static int maxLogins = 0; 		// no limit
	private static int maxAnonLogins = -1;	// disabled
	private static int maxThreads = 0;		// no limit
	
	private static String ftpadmin = "admin";
	
	public static void main(String[] args) throws Exception {
		//String hdftp_home = System.getProperty("java.class.path");
		String hdftp_home = HdFtpServer.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		if(hdftp_home.endsWith("tmp/"))
			hdftp_home = hdftp_home + "../";
		hdftp_home = hdftp_home.substring(0, hdftp_home.lastIndexOf("/") + 1);
		
		CONF_FILE = hdftp_home + CONF_FILE;
		CONF_USER = hdftp_home + CONF_USER;
		CONF_LOG = hdftp_home + CONF_LOG;
		SSL_KEY_FILE = hdftp_home + SSL_KEY_FILE;
		HDFS_SITE = hdftp_home + HDFS_SITE;
		
		System.setProperty("hdftp.log.dir", hdftp_home + "logs");	//	set logs directory
		PropertyConfigurator.configure(CONF_LOG);	// load Logger configure
		loadConfig();
		
		if (args.length > 0)
			System.setProperty("hadoop.home.dir", args[0]);
		else if (System.getProperty("hadoop.home.dir") == null)
			System.setProperty("hadoop.home.dir", hdftp_home);
		//System.setProperty("HADOOP_USER_NAME", "superuser");
		
		if (port != 0 || sslPort != 0) {
			FtpServer server = getConfiguration();
			server.start();
			addShutdownHook(server);
		}
	}

	/**
	 * Load configuration
	 * @throws IOException
	 */
	private static void loadConfig() throws IOException {
		Properties props = new Properties();
		props.load(new FileInputStream(CONF_FILE));

		try {
			port = Integer.parseInt(props.getProperty("port"));
			log.info("port is set. ftp server will be started");
		} catch (Exception e) {
			log.info("port is not set. so ftp server will not be started");
		}

		try {
			sslPort = Integer.parseInt(props.getProperty("ssl-port"));
			log.info("ssl-port is set. ssl server will be started");
		} catch (Exception e) {
			log.info("ssl-port is not set. so ssl server will not be started");
		}

		if (port != 0) {
			passivePorts = props.getProperty("data-ports");
			if (passivePorts == null) {
				log.error("data-ports is not set");
				System.exit(1);
			}
		}

		if (sslPort != 0) {
			sslPassivePorts = props.getProperty("ssl-data-ports");
			sslPassword = props.getProperty("ssl-password");
			if (sslPassivePorts == null) {
				log.error("ssl-data-ports is not set");
				System.exit(1);
			}
			if (sslPassword == null) {
				log.error("ssl-password is not set");
				System.exit(1);
			}
		}
		
		hdfsSuperuser = props.getProperty("hdfs-superuser");
		if (hdfsSuperuser == null) {
			log.error("HDFS superuser is not set");
			System.exit(1);
		}
		
		try {
			maxLogins = Integer.parseInt(props.getProperty("max-logins"));
			log.info("max-logins is seted. it is " + maxLogins);
		} catch (Exception e) {
			log.info("max-logins is not set. default is " + maxLogins);
		}
		
		try {
			maxAnonLogins = Integer.parseInt(props.getProperty("max-anon-logins"));
			log.info("max-anon-logins is seted. it is " + maxAnonLogins);
		} catch (Exception e) {
			log.info("max-anon-logins is not set. default is " + maxAnonLogins);
		}
		
		try {
			maxThreads = Integer.parseInt(props.getProperty("maxThreads"));
			log.info("max-threads is seted. it is " + maxThreads);
		} catch (Exception e) {
			log.info("max-threads is not set. default is " + maxThreads);
		}
		
	}
	
	/**
	 * Get the configuration object.
	 * @param <HdFtpUserManager>
	 * @throws Exception
	 */
	public static FtpServer getConfiguration() throws Exception {
		
		FtpServerFactory ftpserver = new FtpServerFactory();
		ConnectionConfigFactory config = new ConnectionConfigFactory();
		config.setMaxAnonymousLogins(maxAnonLogins);
		config.setAnonymousLoginEnabled((maxAnonLogins == -1) ? false : true);
		config.setMaxLogins(maxLogins);
		config.setMaxThreads(maxThreads);
		ftpserver.setConnectionConfig(config.createConnectionConfig());
		
		HdFtpPropertiesUserManager userManager = new HdFtpPropertiesUserManager(ftpadmin, new Md5PasswordEncryptor(), new File(CONF_USER));
		ftpserver.setUserManager(userManager);
		ftpserver.setFileSystem(new HdfsFileSystemFactory(hdfsSuperuser, HDFS_SITE));
		
		ListenerFactory listener = new ListenerFactory();
		DataConnectionConfigurationFactory dataFactory = new DataConnectionConfigurationFactory();
		
		if (port != 0) {
			log.info("Adding listener of Hdfs-Over-Ftp server. port: " + port + " data-ports: " + passivePorts);
			
			listener.setImplicitSsl(false);
			
			listener.setPort(port);
			dataFactory.setPassivePorts(passivePorts);
			listener.setDataConnectionConfiguration(dataFactory.createDataConnectionConfiguration());

			ftpserver.addListener("default", listener.createListener());
		}
		if (sslPort != 0) {
			log.info("Adding listener of Hdfs-Over-Ftp SSL server. ssl-port: " + sslPort + " ssl-data-ports: " + sslPassivePorts);
			
			// define SSL configuration
			SslConfigurationFactory ssl = new SslConfigurationFactory();
			ssl.setKeystoreFile(new File(SSL_KEY_FILE));
			ssl.setKeystorePassword(sslPassword);
			// set the SSL configuration for the listener
			listener.setSslConfiguration(ssl.createSslConfiguration());
			listener.setImplicitSsl(true);
			
			listener.setPort(sslPort);
			dataFactory.setPassivePorts(sslPassivePorts);
			//dataFactory.setSslConfiguration(ssl.createSslConfiguration());
			//dataFactory.setImplicitSsl(true);							// encrypted data transfer
			listener.setDataConnectionConfiguration(dataFactory.createDataConnectionConfiguration());
	
			ftpserver.addListener("defaultSSL", listener.createListener());
		}
		
		log.info("Starting Hdfs-Over-Ftp server.");
		return ftpserver.createServer();
	}

    /**
     * Add shutdown hook.
     */
    private static void addShutdownHook(final FtpServer engine) {

        // create shutdown hook
        Runnable shutdownHook = new Runnable() {
            public void run() {
                engine.stop();
                log.info("HdFtp server stopped.");
            }
        };

        // add shutdown hook
        Runtime runtime = Runtime.getRuntime();
        runtime.addShutdownHook(new Thread(shutdownHook));
    }
	
}
