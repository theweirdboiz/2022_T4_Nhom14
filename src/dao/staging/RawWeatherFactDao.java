package dao.staging;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;

import dao.IdCreater;
import dao.Procedure;
import dao.control.LogControllerDao;
import db.DbStagingControlConnection;
import ftp.FTPManager;

public class RawWeatherFactDao {
	private static final int SOURCE_VN_ID = 1;

	private static final int SOURCE_VN_EDU_ID = 2;
	Connection connection;
	String procedure;
	CallableStatement callStmt;
	ResultSet rs;

	FTPManager ftpManager;
	LogControllerDao log;

	public RawWeatherFactDao() {
		log = new LogControllerDao();
		connection = DbStagingControlConnection.getIntance().getConnect();
	}

	public boolean loadRawThoiTietVnFact() {
		ftpManager = new FTPManager(SOURCE_VN_ID);
		boolean result = false;
		try {
			int logId = log.getIdByStatus(SOURCE_VN_ID, "EO");
			String path = log.getDestinationByStatus(SOURCE_VN_ID);
			if (path == null) {
				return false;
			} else {
				BufferedReader br = ftpManager.getReaderFileInFTPServer(path);
				String line = br.readLine();
				String[] timeStamp = line.split(" ");
				while ((line = br.readLine()) != null) {
					StringTokenizer stk = new StringTokenizer(line, ",");
					int provinceId = Integer.parseInt(stk.nextToken());
					String provinceName = stk.nextToken().trim();
					String dateLoad = timeStamp[0];
					String timeLoad = timeStamp[1];
					int currentTemp = Integer.parseInt(stk.nextToken().trim());
					String overview = stk.nextToken().trim();
					int lowestTemp = Integer.parseInt(stk.nextToken().trim());
					int maximumTemp = Integer.parseInt(stk.nextToken().trim());
					float humidity = Float.parseFloat(stk.nextToken().trim());
					float vision = Float.parseFloat(stk.nextToken().trim());
					float wind = Float.parseFloat(stk.nextToken().trim());
					int stopPoint = Integer.parseInt(stk.nextToken().trim());
					float uvIndex = Float.parseFloat(stk.nextToken().trim());
					String airQuality = stk.nextToken().trim();

					try {
						procedure = Procedure.LOAD_RAW_WEATHER_DATA_INTO_STAGING;
						callStmt = connection.prepareCall(procedure);
						callStmt.setInt(1, IdCreater.createIdByCurrentTime());
						callStmt.setString(2, provinceName);
						callStmt.setString(3, dateLoad);
						callStmt.setString(4, timeLoad);
						callStmt.setInt(5, currentTemp);
						callStmt.setString(6, overview);
						callStmt.setInt(7, lowestTemp);
						callStmt.setInt(8, maximumTemp);
						callStmt.setFloat(9, humidity);
						callStmt.setFloat(10, vision);
						callStmt.setFloat(11, wind);
						callStmt.setInt(12, stopPoint);
						callStmt.setFloat(13, uvIndex);
						callStmt.setString(14, airQuality);
						result = callStmt.executeUpdate() > 0;
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}

			if (result) {
				log.updateStatus(logId, "EL");
				System.out.println("ThoiTietVn raw fact load successully!");
			} else {
				System.out.println("ThoiTietVn raw fact load not successully!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			ftpManager.close();
		}
		return result;
	}

	public boolean loadRawThoiTietEduVnFact() {
		ftpManager = new FTPManager(SOURCE_VN_EDU_ID);
		boolean result = false;
		try {
			int logId = log.getIdByStatus(SOURCE_VN_EDU_ID, "EO");
			String path = log.getDestinationByStatus(SOURCE_VN_EDU_ID);
			if (path == null) {
				return false;
			} else {
				BufferedReader br = ftpManager.getReaderFileInFTPServer(path);
				String line = br.readLine();
				String[] timeStamp = line.split(" ");
				while ((line = br.readLine()) != null) {
					StringTokenizer stk = new StringTokenizer(line, ",");
					int provinceId = Integer.parseInt(stk.nextToken());
					String provinceName = stk.nextToken().trim();
					String dateLoad = timeStamp[0];
					String timeLoad = timeStamp[1];
					int currentTemp = Integer.parseInt(stk.nextToken().trim());
					String overview = stk.nextToken().trim();
					int lowestTemp = Integer.parseInt(stk.nextToken().trim());
					int maximumTemp = Integer.parseInt(stk.nextToken().trim());
					float humidity = Float.parseFloat(stk.nextToken().trim());
					float vision = Float.parseFloat(stk.nextToken().trim());
					float wind = Float.parseFloat(stk.nextToken().trim());
					int stopPoint = Integer.parseInt(stk.nextToken().trim());
					float uvIndex = Float.parseFloat(stk.nextToken().trim());
					String airQuality = stk.nextToken().trim();

					try {
						procedure = Procedure.LOAD_RAW_WEATHER_DATA_INTO_STAGING;
						callStmt = connection.prepareCall(procedure);
						callStmt.setInt(1, IdCreater.createIdByCurrentTime());
						callStmt.setString(2, provinceName);
						callStmt.setString(3, dateLoad);
						callStmt.setString(4, timeLoad);
						callStmt.setInt(5, currentTemp);
						callStmt.setString(6, overview);
						callStmt.setInt(7, lowestTemp);
						callStmt.setInt(8, maximumTemp);
						callStmt.setFloat(9, humidity);
						callStmt.setFloat(10, vision);
						callStmt.setFloat(11, wind);
						callStmt.setInt(12, stopPoint);
						callStmt.setFloat(13, uvIndex);
						callStmt.setString(14, airQuality);
						result = callStmt.executeUpdate() > 0;
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}

			if (result) {
				log.updateStatus(logId, "EL");
				System.out.println("ThoiTietEduVn raw fact load successully!");
			} else {
				System.out.println("ThoiTietEduVn raw fact load not successully!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			ftpManager.close();
		}
		return result;
	}

	public static void main(String[] args) {

	}
}
