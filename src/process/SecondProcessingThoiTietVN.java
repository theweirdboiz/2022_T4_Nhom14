package process;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import dao.IdCreater;
import dao.Procedure;
import dao.control.DbConfigDao;
import dao.control.SourceConfigDao;
import dao.staging.DateDimDao;
import dao.staging.ProvinceDimDao;
import dao.staging.ThoiTietEduVnDao;
import dao.staging.ThoiTietVnDao;
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

	private ThoiTietVnDao thoiTietVnDao;
	private ThoiTietEduVnDao thoiTietEduVnDao;

//2. Transform
	public SecondProcessingThoiTietVN() {
		// 2.1 Connect FTPConfig -> Lấy thông tin FTP Server -> Connect FTP Server
//		ftpManager = new FTPManager(so);
		dbConfigDao = new DbConfigDao();
		sourceConfigDao = new SourceConfigDao();
		thoiTietEduVnDao = new ThoiTietEduVnDao();
		thoiTietVnDao = new ThoiTietVnDao();
	}

// Kiểm tra các dim đã được load hay chưa?
	public boolean isDimLoaded() throws IOException, NumberFormatException, SQLException {
		DateDimDao dateDimDao = new DateDimDao();
		TimeDimdao timeDimdao = new TimeDimdao();
		ProvinceDimDao provinceDimDao = new ProvinceDimDao();
		return dateDimDao.create() && timeDimdao.create() && provinceDimDao.create();
	}

//Load fact
	public boolean createFact() throws NumberFormatException, IOException, SQLException {
		boolean result = false;
		if (isDimLoaded()) {
			System.out.println("Create dim succesful!");
			result = thoiTietVnDao.createRawData() & thoiTietEduVnDao.createRawData();
		} else {
			System.out.println("Create dim not succesful!");
		}
		if (result) {
			System.out.println("Create raw_weather_fact successful!");
		} else {
			System.out.println("Create raw_weather_fact not successful!");
		}
		return result;
	}

	private boolean transformWeatherFact() {
		boolean result = false;
		dbHosting = dbConfigDao.getStagingHosting();
		connection = new MySQLConnection(dbHosting).getConnect();
		String query = "SELECT * FROM raw_weather_data";
		try {
			PreparedStatement ps = connection.prepareStatement(query);
			rs = ps.executeQuery();
			while (rs.next()) {
				procedure = Procedure.TRANSFORM_WEATHER_FACT;
				callStmt = connection.prepareCall(procedure);
				callStmt.setInt(1, rs.getInt("id"));
				callStmt.setString(2, rs.getString("province_name"));
				callStmt.setString(3, rs.getString("date_load"));
				callStmt.setString(4, rs.getString("time_load"));
				callStmt.setInt(5, rs.getInt("currentTemp"));
				callStmt.setString(6, rs.getString("overview"));
				callStmt.setInt(7, rs.getInt("lowestTemp"));
				callStmt.setInt(8, rs.getInt("maximumTemp"));
				callStmt.setInt(9, rs.getInt("humidity"));
				callStmt.setFloat(10, rs.getFloat("vision"));
				callStmt.setFloat(11, rs.getFloat("wind"));
				callStmt.setInt(12, rs.getInt("stopPoint"));
				callStmt.setInt(13, rs.getInt("uvIndex"));
				callStmt.setString(14, rs.getString("airQuality"));
				callStmt.setString(15, rs.getString("date_load") + " " + rs.getString("time_load"));
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
		if (createFact()) {
			result = transformWeatherFact();
		}
		return result;
	}

	public static void main(String[] args) throws SQLException, IOException {
		SecondProcessingThoiTietVN sp = new SecondProcessingThoiTietVN();
		if (sp.runScript()) {
			System.out.println("Create fact successful!");
		} else {
			System.out.println("Create fact not successful!");
		}
	}
}
