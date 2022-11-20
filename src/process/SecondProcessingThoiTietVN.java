package process;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import dao.IdCreater;
import dao.Procedure;
import dao.control.DbConfigDao;
import dao.control.SourceConfigDao;
import dao.staging.DateDimDao;
import dao.staging.ProvinceDimDao;
import dao.staging.TimeDimdao;
import db.MySQLConnection;
import ftp.FTPManager;
import model.DbHosting;

public class SecondProcessingThoiTietVN implements Procedure {
	private FTPManager ftpManager;
	private CallableStatement callStmt;
	private ResultSet rs;
	private String procedure;
	private Connection connection;
	private DbConfigDao dbConfigDao;
	private DbHosting dbHosting;
	private SourceConfigDao sourceConfigDao;

//2. Transform
	public SecondProcessingThoiTietVN() {
		// 2.1 Connect FTPConfig -> Lấy thông tin FTP Server -> Connect FTP Server
//		ftpManager = new FTPManager(so);
		dbConfigDao = new DbConfigDao();
		sourceConfigDao = new SourceConfigDao();
	}

// Kiểm tra các dim đã được load hay chưa?
	public boolean isDimLoaded() throws IOException, NumberFormatException, SQLException {
		DateDimDao dateDimDao = new DateDimDao();
		TimeDimdao timeDimdao = new TimeDimdao();
		ProvinceDimDao provinceDimDao = new ProvinceDimDao();
		return dateDimDao.create() && timeDimdao.create() && provinceDimDao.create();
	}

//Load fact
	private boolean loadWeatherData() {
		boolean result = false;
		dbHosting = dbConfigDao.getStagingHosting();
		connection = new MySQLConnection(dbHosting).getConnect();
		procedure = Procedure.LOAD_WEATHER_DATA;
		try {
			callStmt = connection.prepareCall(procedure);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	private boolean transformWeatherFact() {
		boolean result = false;
		dbHosting = dbConfigDao.getStagingHosting();
		connection = new MySQLConnection(dbHosting).getConnect();
		String query = "SELECT * FROM weatherfact";
		try {
			PreparedStatement ps = connection.prepareStatement(query);
			rs = ps.executeQuery();
			while (rs.next()) {
				procedure = Procedure.TRANSFORM_WEATHER_FACT;
				callStmt = connection.prepareCall(procedure);
				callStmt.setInt(1, IdCreater.createIdByCurrentTime());
				callStmt.setInt(2, rs.getInt("id"));
				callStmt.setInt(3, rs.getInt("province_id"));
				callStmt.setInt(4, rs.getInt("date_id"));
				callStmt.setInt(5, rs.getInt("time_id"));
				callStmt.setInt(6, rs.getInt("currentTemp"));
				callStmt.setString(7, rs.getString("overview"));
				callStmt.setInt(8, rs.getInt("lowestTemp"));
				callStmt.setInt(9, rs.getInt("maximumTemp"));
				callStmt.setInt(10, rs.getInt("humidity"));
				callStmt.setFloat(11, rs.getFloat("vision"));
				callStmt.setFloat(12, rs.getFloat("wind"));
				callStmt.setInt(13, rs.getInt("stopPoint"));
				callStmt.setInt(14, rs.getInt("uvIndex"));
				callStmt.setString(15, rs.getString("airQuality"));
				result = callStmt.execute();
			}
//			callStmt = connection.prepareCall(procedure);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		return result;
	}

	public boolean runScript() throws SQLException, IOException {
		boolean result = false;
		// Kiểm tra các dim đã load vào staging hay chưa?
		if (isDimLoaded()) {
//			if (loadThoiTietVN() && loadThoiTietEduVN() && loadProvinceDim()) {
//				result = loadWeatherData();
//			}
		} else {
			result = false;
		}
		if (result) {
			transformWeatherFact();
		}
		return result;
	}

	public static void main(String[] args) throws SQLException, IOException {
		SecondProcessingThoiTietVN sp = new SecondProcessingThoiTietVN();
		System.out.println(sp.isDimLoaded());
//		sp.transformWeatherFact();
//		ProvinceDimDao.createProvinceDim();
//		sp.loadTimeDim();
//		sp.loadThoiTietEduVN();
//		sp.loadThoiTietVN();
//		sp.runScript();
	}
}
