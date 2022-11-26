package dao.staging;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.StringTokenizer;

import dao.Procedure;
import dao.control.DbConfigDao;
import dao.control.SourceConfigDao;
import db.DbControlConnection;
import db.DbStagingControlConnection;
import db.MySQLConnection;
import ftp.FTPManager;
import model.DbHosting;

public class ProvinceDimDao {

	private Connection connection;
	private CallableStatement callStmt;
	private String procedure;
	private int sourceId = 3;

	public ProvinceDimDao() {
		connection = DbStagingControlConnection.getIntance().getConnect();
	}

	public boolean loadByLine(int id, String name) {
		boolean result = false;
		procedure = Procedure.LOAD_PROVINCE_DIM;
		try {
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, id);
			callStmt.setString(2, name);
			result = callStmt.executeUpdate() > 0;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return result;
	}

	public boolean checkInFTP() throws SQLException {
		connection = DbControlConnection.getIntance().getConnect();
		procedure = Procedure.GET_ONE_FILE_IN_FTP;
		callStmt = connection.prepareCall(procedure);
		callStmt.setInt(1, sourceId);
		return callStmt.execute();
	}

	public ResultSet getAll() {
		connection = DbStagingControlConnection.getIntance().getConnect();
		procedure = Procedure.GET_ALL_PROVINCEDIM_IN_STAGING;
		try {
			callStmt = connection.prepareCall(procedure);
			return callStmt.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public boolean insert(String line) {
		connection = DbStagingControlConnection.getIntance().getConnect();
		String[] elms = line.split(",");
		procedure = Procedure.LOAD_PROVINCE_DIM;
		try {
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, Integer.parseInt(elms[0].trim()));
			callStmt.setString(2, elms[1].trim());
			return callStmt.executeUpdate() > 0;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static void main(String[] args) throws SQLException {
		ProvinceDimDao provinceDimDao = new ProvinceDimDao();
//		System.out.println(provinceDimDao.checkInFTP());
	}
}
