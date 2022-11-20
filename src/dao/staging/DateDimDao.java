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
import dao.control.DbConfigDao;
import db.MySQLConnection;
import model.DbHosting;

public class DateDimDao {
	public static final String OUT_FILE = "date_dim.csv";
	public static final int NUMBER_OF_RECORD = 5432;
	public static final String TIME_ZONE = "PST8PDT";
	public File file = new File(OUT_FILE);

	private boolean createFile() {
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

	public boolean create() throws NumberFormatException, IOException, SQLException {
		DbConfigDao dbConfigDao = new DbConfigDao();
		DbHosting dbHosting = dbConfigDao.getStagingHosting();
		Connection connection = new MySQLConnection(dbHosting).getConnect();
		boolean result = false;
		// Kiểm tra datedim đã tồn tại trong staging chưa?
		String procedure = Procedure.CHECK_DATE_DIM_IS_EXISTED;
		boolean checkIsExisted = false;
		try {
			CallableStatement callStmt = connection.prepareCall(procedure);
			ResultSet rs = callStmt.executeQuery();
			checkIsExisted = rs.next();
			// Nếu đã tồn tại thì kết thúc luôn
			if (checkIsExisted) {
				System.out.println("Datedim has been existed!");
				return true;
			}
			// Nếu chưa tạo mới rồi insert vào staging
			if (!createFile()) {
				System.out.println("Can't create file datedim.csv");
				return false;
			}
			BufferedReader lineReader = new BufferedReader(new FileReader(file));
			String lineText = null;
			while ((lineText = lineReader.readLine()) != null) {
				String[] data = lineText.split(",");
				int id = Integer.parseInt(data[0].trim());
				String date = data[1].trim();
				int year = Integer.parseInt(data[2].trim());
				int month = Integer.parseInt(data[3].trim());
				int day = Integer.parseInt(data[4].trim());
				String dayOfWeek = data[5].trim();

				procedure = Procedure.LOAD_DATE_DIM;
				callStmt = connection.prepareCall(procedure);
				callStmt.setInt(1, id);
				callStmt.setString(2, date);
				callStmt.setInt(3, year);
				callStmt.setInt(4, month);
				callStmt.setInt(5, day);
				callStmt.setString(6, dayOfWeek);
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
		DateDimDao dao = new DateDimDao();
		if (dao.create()) {
			System.out.println("Create datedim successful!");
		}
	}
}
