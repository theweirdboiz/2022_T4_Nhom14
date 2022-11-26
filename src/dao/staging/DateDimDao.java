package dao.staging;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import dao.Procedure;
import db.DbStagingControlConnection;

public class DateDimDao {
	public static final String OUT_FILE = "date_dim.csv";
	public static final int NUMBER_OF_RECORD = 9876;
	public static final String TIME_ZONE = "PST8PDT";
	public File file = new File(OUT_FILE);

	private static final int SOURCE_ID = 4;

	private Connection connection;
	private CallableStatement callStmt;
	private String procedure;
	private ResultSet rs;

	public DateDimDao() {
		connection = DbStagingControlConnection.getIntance().getConnect();
	}

	public boolean isDateDimExisted() {
		boolean result = false;
		try {
			System.out.println(">> Start: check date dim is existed");
			procedure = Procedure.CHECK_DATE_DIM_IS_EXISTED;
			callStmt = connection.prepareCall(procedure);
			rs = callStmt.executeQuery();
			result = rs.next();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println(">> End: " + result);
		return result;
	}

	public boolean loadByLine(int id, String date, int year, int month, int day, String dayOfWeek) {
		boolean result = false;
		procedure = Procedure.LOAD_DATE_DIM;
		try {
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, id);
			callStmt.setString(2, date);
			callStmt.setInt(3, year);
			callStmt.setInt(4, month);
			callStmt.setInt(5, day);
			callStmt.setString(6, dayOfWeek);
			result = callStmt.executeUpdate() > 0;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

	public boolean createFile() {
		DateTimeZone dateTimeZone = DateTimeZone.forID(TIME_ZONE);
		int count = 0;
		int date_sk = 0;
		PrintWriter pr = null;
		try {
			if (file.exists()) {
				file.delete();
			}
			pr = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
		} catch (Exception e) {
			e.printStackTrace();
		}
		DateTime startDateTime = new DateTime(2022, 10, 31, 0, 0, 0);
		while (count <= NUMBER_OF_RECORD) {
			startDateTime = startDateTime.plus(Period.days(1));
			Date startDate = startDateTime.toDate();
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(startDate);
			// Date_SK
			date_sk += 1; // 1
			SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd");
			// Full Date
			String full_date = dt.format(calendar.getTime()); // 2
			dt = new SimpleDateFormat("yyyy");
			// Calendar Year
			String calendar_year = dt.format(calendar.getTime()); // 7
			// Calendar Month
			int calendar_month = calendar.MONTH;
			int calendar_day_of_month = calendar.get(Calendar.DAY_OF_MONTH); // 9
			// Calendar day_of_week
			String calendar_day_of_week = calendar.getDisplayName(Calendar.DAY_OF_WEEK, Calendar.LONG, Locale.US); // 5
			String output = date_sk + "," + full_date + "," + calendar_year + "," + calendar_month + ", "
					+ calendar_day_of_month + ", " + calendar_day_of_week;
			count++;
			// Printout Data to File
			pr.println(output);
			pr.flush();
		}
		return file.length() > 0;
	}

	public static void main(String[] args) throws NumberFormatException, IOException, SQLException {
		DateDimDao dateDimDao = new DateDimDao();
	}
}
