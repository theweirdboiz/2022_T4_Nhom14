package dao.staging;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.StringTokenizer;

import dao.Procedure;
import dao.control.DbConfigDao;
import dao.control.SourceConfigDao;
import db.DbControlConnection;
import db.MySQLConnection;
import ftp.FTPManager;
import model.DbHosting;

public class  ProvinceDimDao {
	public static final int SOURCE_ID_PROVINCE_DIM = 3;
	public static final String EXTENSION = ".csv";

	public  boolean create() {
		boolean result = false;
		FTPManager ftpManager = new FTPManager(SOURCE_ID_PROVINCE_DIM);
		Connection connection = DbControlConnection.getIntance().getConnect();

		try {
			String procedure = Procedure.GET_ONE_FILE_IN_FTP;
			CallableStatement callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, SOURCE_ID_PROVINCE_DIM);
			ResultSet rs = callStmt.executeQuery();
			// 1.1.1 Nếu không có file nào mới -> kết thúc
			if (!rs.next()) {
				System.out.println("Không có file mới");
				return true;
			}
			// 1.1.2 Nếu có -> lấy thông tin từ dòng dữ liệu này: logId, status, sourceId,
			// timeLoad,dateLoad
			int logId = rs.getInt("id");
			String dateLoad = rs.getString("dateLoad");
			String timeLoad = rs.getString("timeLoad");
			String mySeperator = "_";
			String fileSeperator = "/";
			String folderName = dateLoad;
			String fileName = "";
			if (Integer.parseInt(timeLoad) < 10) {
				DecimalFormat formatter = new DecimalFormat("00");
				fileName = folderName + mySeperator + formatter.format(Integer.parseInt(timeLoad)) + EXTENSION;
			} else {
				fileName = folderName + mySeperator + timeLoad + EXTENSION;
			}
			SourceConfigDao sourceConfigDao = new SourceConfigDao();
			// 1.1.3 -> vào ftp server và lấy file này xuống
			String path = sourceConfigDao.getDistFolder(SOURCE_ID_PROVINCE_DIM) + fileSeperator + folderName
					+ fileSeperator + fileName;
			try {
				// 1.1.4 get info db statging
				DbConfigDao dbConfigDao = new DbConfigDao();
				DbHosting dbHosting = dbConfigDao.getStagingHosting();
				connection = new MySQLConnection(dbHosting).getConnect();
				// 1.1.5 Mở file
				BufferedReader br = ftpManager.getReaderFileInFTPServer(path);
				String rowData;
				// 1.1.6 Read by line
				LOOP: while ((rowData = br.readLine()) != null) {
					StringTokenizer stk = new StringTokenizer(rowData, ",");
					String provinceName = null;
					int id = Integer.parseInt(stk.nextToken().trim());
					provinceName = stk.nextToken().trim();
					// 1.1.7 Load into staging.raw_weather_data by line
					procedure = Procedure.LOAD_PROVINCE_DIM;
					callStmt = connection.prepareCall(procedure);
					callStmt.setInt(1, id);
					callStmt.setString(2, provinceName);
					result = callStmt.executeUpdate() > 0;
				}
				// Kiểm tra kết quả quá trình load
				connection = DbControlConnection.getIntance().getConnect();
				if (result) {
					// Update stt EL
					procedure = Procedure.UPDATE_STATUS;
					callStmt = connection.prepareCall(procedure);
					callStmt.setString(1, "EL");
					callStmt.setInt(2, logId);
					callStmt.execute();
					System.out.println("Load data into staging.provinceDim thành công!");
					connection.close();
					return true;
				} else {
					System.out.println("Load data into staging.provinceDim ko thành công!");
					connection.close();
					return false;
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Chắc là đường dẫn này không tồn tại!");
				connection.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}
	public static void main(String[] args) {
		ProvinceDimDao provinceDimDao = new ProvinceDimDao();
		provinceDimDao.create();
	}
}
