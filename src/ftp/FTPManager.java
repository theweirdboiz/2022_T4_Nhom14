package ftp;

import org.apache.commons.net.ftp.FTPClient;

import dao.control.FTPConfigDao;
import db.FTPConnection;

public class FTPManager {
	private FTPClient ftpClient;
	private FTPConfigDao ftpConfigDao;

	public FTPManager() {
		ftpConfigDao = new FTPConfigDao();
		ftpClient = new FTPConnection(ftpConfigDao.getFTPHosting()).getClient();
	}
	
	public boolean pushFile(String path, String dist) {
		return false;
	}
	
	
}
