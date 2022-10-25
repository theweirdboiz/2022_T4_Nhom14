package process;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.CallableStatement;
import java.sql.SQLException;

import org.apache.commons.net.ftp.FTPClient;

import com.mysql.cj.protocol.Resultset;

import db.MySQLConnection;

public class SecondProcessing {
	FTPClient ftpClient;
	private static final int BUFFER_SIZE = 1024 * 1024 * 1;
	private static final String FTP_USERNAME = "ngsfihae";
	private static final String FTP_PASSWORD = "U05IIKw0HsICPNU";
	private static final String SLASH = "/";

	private final String WEB_URL = "https://vi.wikipedia.org/wiki/T%E1%BB%89nh_th%C3%A0nh_Vi%E1%BB%87t_Nam";
	private String DB_URL = "jdbc:mysql://localhost:3306/control";
	private final String USER_NAME = "root";
	private final String PASSWORD = "";
	MySQLConnection connectDb;

	MySQLConnection mySQLConnection;

	public SecondProcessing() {
		// 1. Connect db control
		mySQLConnection = new MySQLConnection(DB_URL, USER_NAME, PASSWORD);
	}

	public void runScript() throws SQLException {
		// 2. Check log
		String query = "{CALL CHECK_DATA_TODAY()}";
		CallableStatement callStmt = mySQLConnection.getConnect().prepareCall(query);
		Resultset rs = (Resultset) callStmt.executeQuery();
		boolean check = !rs.getRows().isEmpty();
		if (check) {
			// 3. Download file FTP
			boolean isSuccess = downloadFTPFile("filePath", "./");
			if (isSuccess) {
				// 3.1 get file already have been download from FTP
				// 4. load staging
				DB_URL = "jdbc:mysql://localhost:3306/staging";
				mySQLConnection = new MySQLConnection(DB_URL, USER_NAME, PASSWORD);
				loadingToStaging();
				// 5.1 update log
			} else {
				// 52. update log
			}
		}
	}

	public void loadingToStaging() {
		// call procedure loading
		// write by line

	}

	// 1. Download file from FTP
	private boolean downloadFTPFile(String ftpFilePath, String downloadFilePath) {
		System.out.println("File " + ftpFilePath + " is downloading...");
		OutputStream outputStream = null;
		boolean success = false;
		try {
			File downloadFile = new File(downloadFilePath);
			outputStream = new BufferedOutputStream(new FileOutputStream(downloadFile));
			// download file from FTP Server
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			ftpClient.setBufferSize(BUFFER_SIZE);
			success = ftpClient.retrieveFile(ftpFilePath, outputStream);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				outputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (success) {
			System.out.println("File " + ftpFilePath + " has been downloaded successfully.");
		} else {
		}
		return success;
	}

	public static void main(String[] args) throws SQLException {
		SecondProcessing sp = new SecondProcessing();
		sp.runScript();
	}
}
