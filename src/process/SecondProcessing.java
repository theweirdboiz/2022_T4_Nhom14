package process;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import dao.Procedure;
import db.DbControlConnection;
import db.DbStagingConnection;
import ftp.FTPManager;

public class SecondProcessing implements Procedure {
	private FTPManager ftpManager;
	private CallableStatement callStmt;
	private ResultSet rs;
	private String procedure;
	private Connection connection;

//2. Loading to Staging
	public SecondProcessing() {
		// 2.1 Connect FTPConfig -> Lấy thông tin FTP Server -> Connect FTP Server
		ftpManager = new FTPManager();
		connection = DbControlConnection.getIntance().getConnect();

	}

	public void runScript() throws SQLException {
		// 2.2 Lấy source id nào có trạng thái 'EO' và ngày ghi log = ngày hôm nay
		procedure = Procedure.CHECK_FILE_CURRENT_IN_FTP_SERVER;
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd");
		LocalDateTime now = LocalDateTime.now();
		String ext = "_data.csv";
		try {
			callStmt = connection.prepareCall(procedure);
			rs = callStmt.executeQuery();
			rs.next();
			int currentFileId = rs.getInt(1);
//			2.3 downloadFile -> Lấy thông tin từ dbConfig table -> Connect db Staging
			connection = DbStagingConnection.getIntance().getConnect();
			try {
				BufferedReader br = ftpManager.getReaderFileInFTPServer(now + ext);
				String line;
				while ((line = br.readLine()) != null) {
					// LOAD BY LINE
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws SQLException {
		SecondProcessing sp = new SecondProcessing();
		sp.runScript();
	}
}
