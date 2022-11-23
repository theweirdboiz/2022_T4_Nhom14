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

	public TimeDimdao() {
		connection = DbStagingControlConnection.getIntance().getConnect();
	}

	private boolean createFile() throws FileNotFoundException {
		PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file)));
		int count = 1;
		for (int i = 0; i < 24; i++) {
			for (int j = 0; j < 60; j++) {
				writer.println(count++ + "," + (i + ":" + j));
			}
		}
		writer.flush();
		return file.length() > 0;
	}
	public boolean getAll() throws SQLException {
		procedure = Procedure.CHECK_TIME_DIM_IS_EXISTED;
		boolean result = false;
		try {
			callStmt = connection.prepareCall(procedure);
			result = callStmt.executeQuery().next();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}
	public boolean insert() throws SQLException, NumberFormatException, FileNotFoundException, IOException {
		String line;
		boolean result = false;
		if (getAll()) {
			System.out.println("Timedim has been loaded into staging!");
			return true;
		}
		if (createFile()) {
			try {
				BufferedReader br = new BufferedReader(new FileReader(new File(OUT_FILE)));
				while ((line = br.readLine()) != null) {
					String[] elms = line.split(",");
					procedure = Procedure.LOAD_DATE_DIM;
					callStmt = connection.prepareCall(procedure);
					callStmt.setInt(1, Integer.parseInt(elms[0].trim()));
					callStmt.setString(2, elms[1].trim());
					callStmt.setInt(3, Integer.parseInt(elms[2].trim()));
					callStmt.setInt(4, Integer.parseInt(elms[3].trim()));
					callStmt.setInt(5, Integer.parseInt(elms[4].trim()));
					callStmt.setString(6, elms[5].trim());
					result = callStmt.executeUpdate() > 0;
				}
				br.close();
				return result;
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		if (result) {
			System.out.println("Timedim load into staging successfully!");
		}
		return false;
	}

	public static void main(String[] args) throws NumberFormatException, IOException, SQLException {
		
	}
}
