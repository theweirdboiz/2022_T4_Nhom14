package ftp;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import dao.CurrentTimeStamp;
import dao.control.FTPConfigDao;
import db.FTPConnection;

public class FTPManager {
	private static FTPClient ftpClient;
	private FTPConfigDao ftpConfigDao;

	public FTPManager() {
		ftpConfigDao = new FTPConfigDao();
		ftpClient = new FTPConnection(ftpConfigDao.getFTPHosting()).getClient();
	}

	public boolean checkDirectoryExists(String distFolder) throws IOException {
		ftpClient.changeWorkingDirectory(distFolder);
		int returnCode = ftpClient.getReplyCode();
		if (returnCode == 550) {
			return false;
		}
		return true;
	}

	public boolean pushFile(String path, String distFolder, String fileName) throws IOException {
//		System.out.println("path:" + path);
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File(path)));
		distFolder = distFolder + "/" + CurrentTimeStamp.getCurrentDate();
//		System.out.println("folder ftp: " + distFolder);
		ftpClient.makeDirectory(distFolder);
		return ftpClient.storeFile(distFolder + "/" + fileName, bis);

	
	public boolean pushFile(String path, String distFolder, String fileName) throws IOException {
		FileInputStream fis = new FileInputStream(new File(path));
		ftpClient.makeDirectory(distFolder);
		return ftpClient.storeFile(distFolder + "/" + fileName, fis);
	}

	public BufferedReader getReaderFileInFTPServer(String path) throws IOException {
		return new BufferedReader(new InputStreamReader(ftpClient.retrieveFileStream(path)));
	}

	public FTPClient getClient() {
		return this.ftpClient;
	}

	
	public void close() {
		try {
			ftpClient.disconnect();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void listFolder(FTPClient ftpClient, String remotePath) throws IOException {
		System.out.println("Danh sách file của [" + remotePath + "]: ");
		FTPFile[] remoteFiles = ftpClient.listFiles(remotePath);
		for (FTPFile remoteFile : remoteFiles) {
			if (!remoteFile.getName().equals(".") && !remoteFile.getName().equals("..")) {
				String remoteFilePath = remotePath + "/" + remoteFile.getName();

				if (remoteFile.isDirectory()) {
					listFolder(ftpClient, remoteFilePath);
				} else {
					System.out.println("* File: " + remoteFilePath);
				}
			}
		}
	}

	public static void main(String[] args) throws IOException {
		FTPManager ftpManager = new FTPManager();
//		System.out.println(ftpManager.checkDirectoryExists("weather_extract/2022-11-06"));
//		ftpManager.getClient().makeDirectory("weather_extract/2022-11-06");
//		System.out.println(("weather_extract/2022-11-07/2022-11-07_00.csv"));
//		ftpClient.removeDirectory("weather_extract/2022-11-06");
//		ftpClient.deleteFile("weather_extract/2022-11-07/2022-11-07_00.csv");
		ftpManager.listFolder(ftpClient, "weather_extract/2022-11-07");
//		System.out.println(ftpManager.checkDirectoryExists("weather_extract/2022-11-07"));
	}

			e.printStackTrace();
		}
	}
}
