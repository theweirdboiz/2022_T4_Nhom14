package dao.warehouse;

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
import db.DbWarehouseConnection;
import db.MySQLConnection;
import ftp.FTPManager;
import model.DbHosting;

public class ProvinceDimDao {

	private Connection connection;
	private CallableStatement callStmt;
	private String procedure;
	private int sourceId = 3;

	public ProvinceDimDao() {
	}

//	public boolean checkInFTP() throws SQLException {
//		connection = DbControlConnection.getIntance().getConnect();
//		procedure = Procedure.GET_ONE_FILE_IN_FTP;
//		callStmt = connection.prepareCall(procedure);
//		callStmt.setInt(1, sourceId);
//		return callStmt.execute();
//	}
//	public boolean checkSlowlyChange() {
//
//	}

	public boolean insert(int sk, int id, String nameProvince) {
		connection = DbWarehouseConnection.getIntance().getConnect();
		procedure = Procedure.LOAD_PROVINCE_DIM_INTO_WAREHOUSE;
		try {
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, sk);
			callStmt.setInt(2, id);
			callStmt.setString(3, nameProvince);
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
