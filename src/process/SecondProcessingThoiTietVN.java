package process;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import dao.CreateDateDim;
import dao.Procedure;
import dao.control.DbConfigDao;
import db.DbControlConnection;
import db.MySQLConnection;
import ftp.FTPManager;
import model.DbHosting;

public class SecondProcessingThoiTietVN implements Procedure, CreateDateDim {
	private FTPManager ftpManager;
	private CallableStatement callStmt;
	private ResultSet rs;
	private String procedure;
	private Connection connection;
	private DbConfigDao dbConfigDao;
	private DbHosting dbHosting;

//2. Loading to Staging
	public SecondProcessingThoiTietVN() {
		// 2.1 Connect FTPConfig -> Lấy thông tin FTP Server -> Connect FTP Server
		ftpManager = new FTPManager();
		connection = DbControlConnection.getIntance().getConnect();
		dbConfigDao = new DbConfigDao();
		dbHosting = dbConfigDao.getStaggingHosting();
	}

	public void loadDateDim() throws IOException {
		connection = new MySQLConnection(dbHosting).getConnect();

		if (CreateDateDim.create()) {
			try {
				BufferedReader lineReader = new BufferedReader(new FileReader(CreateDateDim.file));
				String lineText = null;
				procedure = Procedure.DELETE_DATE_DIM;
				// delete all data in datedim
				try {
					callStmt = connection.prepareCall(procedure);
					callStmt.execute();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				// insert new data
				while ((lineText = lineReader.readLine()) != null) {
					String[] data = lineText.split(",");
					int id = Integer.parseInt(data[0].trim());
					String date = data[1].trim();
					int year = Integer.parseInt(data[2].trim());
					int month = Integer.parseInt(data[3].trim());
					int day = Integer.parseInt(data[4].trim());
					String dayOfWeek = data[5].trim();

					System.out.println(id + " " + date + " " + year + " " + month + " " + day + " " + dayOfWeek);
					try {
						procedure = Procedure.LOAD_DATE_DIM;
						callStmt = connection.prepareCall(procedure);
						callStmt.setInt(1, id);
						callStmt.setString(2, date);
						callStmt.setInt(3, year);
						callStmt.setInt(4, month);
						callStmt.setInt(5, day);
						callStmt.setString(6, dayOfWeek);
						callStmt.execute();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}

				lineReader.close();
			} catch (FileNotFoundException e) {
				try {
					connection.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				e.printStackTrace();
			}
		}

	}

	private void loadProvinceDim() {

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
//			connection = .getIntance().getConnect();
			try {
				BufferedReader br = ftpManager.getReaderFileInFTPServer(now + ext);
				String line;
				while ((line = br.readLine()) != null) {
					// LOAD BY LINE
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws SQLException, IOException {
		SecondProcessingThoiTietVN sp = new SecondProcessingThoiTietVN();
//		sp.runScript();
		sp.loadDateDim();
	}
}
