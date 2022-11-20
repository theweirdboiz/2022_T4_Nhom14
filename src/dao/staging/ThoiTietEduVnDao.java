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

public class ThoiTietEduVnDao {
	private static final int SOURCE_ID = 2;
	private static final String EXTENSION = ".csv";

	private boolean createRawData() {
		boolean result = false;
		FTPManager ftpManager = new FTPManager(SOURCE_ID);
		// 1. Connect db control
		Connection connection = DbControlConnection.getIntance().getConnect();
		// 2.4 Lấy id có trạng thái 'EO' và ngày ghi log = ngày hôm nay cập nhật sang
		// stt 'EL'
		// 1.1 Lấy một dòng dữ liệu
		// current_date and status ='EO' AND ID=SOURCE_ID_THOI_TIET_VN
		try {
			String procedure = Procedure.GET_ONE_FILE_IN_FTP;
			CallableStatement callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, SOURCE_ID);
			ResultSet rs = callStmt.executeQuery();
			// 1.1.1 Nếu không có file nào mới -> kết thúc
			if (!rs.next()) {
				System.out.println("Không có file mới");
				return result;
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
			// 1.1.3 -> vào ftp server và lấy file này xuống
			SourceConfigDao sourceConfigDao = new SourceConfigDao();
			String path = sourceConfigDao.getDistFolder(SOURCE_ID) + fileSeperator + folderName + fileSeperator
					+ fileName;
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
					String dateLoadData = null;
					Integer timeLoadData = null;
					Integer currentTemp = null;
					String overview = null;
					Integer minTemp = null;
					Integer maxTemp = null;
					Float humidity = null;
					Float vision = null;
					Float wind = null;
					Integer stopPoint = null;
					Float uvIndex = null;
					String airQuality = null;

					int id = Integer.parseInt(stk.nextToken().trim());
					provinceName = stk.nextToken().trim();
					dateLoadData = stk.nextToken().trim();
					timeLoadData = Integer.parseInt(stk.nextToken().trim().split(":")[0]);
					currentTemp = Integer.parseInt(stk.nextToken().trim());
					overview = stk.nextToken().trim();
					minTemp = Integer.parseInt(stk.nextToken().trim());
					maxTemp = Integer.parseInt(stk.nextToken().trim());
					humidity = Float.parseFloat(stk.nextToken().trim());
					vision = Float.parseFloat(stk.nextToken().trim());
					wind = Float.parseFloat(stk.nextToken().trim());
					stopPoint = Integer.parseInt(stk.nextToken().trim());
					uvIndex = Float.parseFloat(stk.nextToken().trim());
					airQuality = stk.nextToken().trim();
					// 1.1.7 Load into staging.raw_weather_data by line
					procedure = Procedure.LOAD_THOI_TIET_EDU_VN_INTO_STAGING;
					callStmt = connection.prepareCall(procedure);
					callStmt.setInt(1, id);
					callStmt.setString(2, provinceName);
					callStmt.setString(3, dateLoadData);
					callStmt.setInt(4, timeLoadData);
					callStmt.setInt(5, currentTemp);
					callStmt.setString(6, overview);
					callStmt.setInt(7, minTemp);
					callStmt.setInt(8, maxTemp);
					callStmt.setFloat(9, humidity);
					callStmt.setFloat(10, vision);
					callStmt.setFloat(11, wind);
					callStmt.setInt(12, stopPoint);
					callStmt.setFloat(13, uvIndex);
					callStmt.setString(14, airQuality);
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
					System.out.println("Create staging.raw_weather_data_thoitieteduvn successful!");
					return true;
				} else {
					System.out.println("Create staging.raw_weather_data_thoitieteduvn not successful!");
					return false;
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Chắc là đường dẫn này không tồn tại!");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	public boolean createFact() {
		boolean result = false;
		return result;
	}

	public static void main(String[] args) {
		ThoiTietEduVnDao thoiTietEduVnDao = new ThoiTietEduVnDao();
		thoiTietEduVnDao.createRawData();
	}
}
