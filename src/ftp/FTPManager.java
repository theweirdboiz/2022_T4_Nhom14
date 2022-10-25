package ftp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

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

	public boolean pushFile(String path, String dist) throws IOException {
		FileInputStream fis = new FileInputStream(new File(path));
		return ftpClient.appendFile(dist, fis);
	}

	public BufferedReader getReaderFileInFTPServer(String path) throws IOException {
		return new BufferedReader(new InputStreamReader(ftpClient.retrieveFileStream(path)));
	}

}
