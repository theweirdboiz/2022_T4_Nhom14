package db;

import java.io.IOException;
import java.net.SocketException;

import org.apache.commons.net.ftp.FTPClient;

import model.FTPHosting;

public class FTPConnection {
	private FTPClient client;
	
	public FTPConnection(String host, int port
			, String username, String password)  {
		try {
			client = new FTPClient();
			client.connect(host, port);
			client.login(username, password);
			client.enterLocalPassiveMode();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public FTPConnection(FTPHosting ftpHosting) {
		try {
			client = new FTPClient();
			client.connect(ftpHosting.getHost(), ftpHosting.getPort());
			client.login(ftpHosting.getUsername(), ftpHosting.getPassword());
			client.enterLocalPassiveMode();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public FTPClient getClient() {
		return client;
	}
}
