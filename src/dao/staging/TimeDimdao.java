package dao.staging;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.StringTokenizer;

import dao.Procedure;
import dao.control.DbConfigDao;
import db.DbStagingControlConnection;
import db.MySQLConnection;
import model.DbHosting;

public class TimeDimdao {
	public static final String OUT_FILE = "time_dim.csv";
	public static final int NUMBER_OF_RECORD = 5432;
	public static final String TIME_ZONE = "PST8PDT";
	public File file = new File(OUT_FILE);

	private Connection connection;
	private CallableStatement callStmt;
	private String procedure;
	private ResultSet rs;

	public TimeDimdao() {
		connection = DbStagingControlConnection.getIntance().getConnect();
	}

	public boolean isTimeDimExisted() {
		boolean result = false;
		try {
			System.out.println(">> Start: check time dim is existed");
			procedure = Procedure.CHECK_TIME_DIM_IS_EXISTED;
			callStmt = connection.prepareCall(procedure);
			rs = callStmt.executeQuery();
			result = rs.next();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println(">> End: " + result);
		return result;
	}

	public boolean loadByLine(int id, String name) {
		boolean result = false;
		procedure = Procedure.LOAD_TIME_DIM;
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

	public boolean createFile() {
		PrintWriter writer;
		try {
			writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file)));
			int count = 1;
			for (int i = 0; i < 24; i++) {
				for (int j = 0; j < 60; j++) {
					writer.println(count++ + "," + (i + ":" + j));
				}
			}
			writer.flush();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return file.length() > 0;
	}

	public static void main(String[] args) throws NumberFormatException, IOException, SQLException {

	}
}
