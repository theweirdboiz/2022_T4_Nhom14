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
import db.MySQLConnection;
import model.DbHosting;

public class TimeDimdao {
	public static final String OUT_FILE = "time_dim.csv";
	public static final int HOUR = 24;
	public static final int MINUTE = 60;
	public static final int SECOND = 60;

	public static final String TIME_ZONE = "PST8PDT";
	public File file = new File(OUT_FILE);

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

	public boolean create() throws NumberFormatException, IOException, SQLException {
		DbConfigDao dbConfigDao = new DbConfigDao();
		DbHosting dbHosting = dbConfigDao.getStagingHosting();
		Connection connection = new MySQLConnection(dbHosting).getConnect();
		boolean result = false;
		// Kiểm tra datedim đã tồn tại trong staging chưa?
		String procedure = Procedure.CHECK_TIME_DIM_IS_EXISTED;
		boolean checkIsExisted = false;
		try {
			CallableStatement callStmt = connection.prepareCall(procedure);
			ResultSet rs = callStmt.executeQuery();
			checkIsExisted = rs.next();
			// Nếu đã tồn tại thì kết thúc luôn
			if (checkIsExisted) {
				System.out.println("Timedim has been existed!");
				return true;
			}
			// Nếu chưa tạo mới rồi insert vào staging
			if (!createFile()) {
				System.out.println("Can't create file timedim.csv");
				return false;
			}
			BufferedReader lineReader = new BufferedReader(new FileReader(file));
			String lineText = null;
			while ((lineText = lineReader.readLine()) != null) {
				StringTokenizer stk = new StringTokenizer(lineText, ",");
				procedure = Procedure.LOAD_TIME_DIM;
				callStmt = connection.prepareCall(procedure);
				callStmt.setInt(1, Integer.parseInt(stk.nextToken()));
				callStmt.setString(2, stk.nextToken());
				result = callStmt.executeUpdate() > 0;
			}
			lineReader.close();
		} catch (SQLException e) {
			System.out.println("Can't create datedim!");
			e.printStackTrace();
		}
		connection.close();
		return result;
	}

	public static void main(String[] args) throws NumberFormatException, IOException, SQLException {
		TimeDimdao dao = new TimeDimdao();
		if (dao.create()) {
			System.out.println("Create timedim successful!");
		}
	}
}
