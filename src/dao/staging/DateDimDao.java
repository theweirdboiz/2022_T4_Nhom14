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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import dao.CurrentTimeStamp;
import dao.IdCreater;
import dao.Procedure;
import dao.control.LogControllerDao;
import dao.control.SourceConfigDao;
import db.DbControlConnection;
import db.DbStagingControlConnection;
import ftp.FTPManager;

public class DateDimDao {
	private Connection connection;
	private CallableStatement callStmt;
	private ResultSet rs;
	private String procedure;
	private PreparedStatement ps;
	private String extension = ".csv";
	private String sourceUrl;
	private SourceConfigDao sourceConfigDao;
	private static final int SOURCE_ID = 4;
	private FTPManager ftpManager;
	public static final String TIME_ZONE = "PST8PDT";
	public static final int NUMBER_OF_RECORD = 8765;

	private PrintWriter writer;
	private LogControllerDao logDao;

	private int logId;
	File ftpFolder, localFolder;
	String ftpPath, localPath, fileName;

	public DateDimDao() {
		connection = DbStagingControlConnection.getIntance().getConnect();
		sourceConfigDao = new SourceConfigDao();
		sourceUrl = sourceConfigDao.getURL(SOURCE_ID);
		ftpManager = new FTPManager();
		logDao = new LogControllerDao();
		logId = IdCreater.createIdByCurrentTime();

		fileName = CurrentTimeStamp.getCurrentTimeStamp();
		ftpFolder = new File(sourceConfigDao.getFtpFolder(SOURCE_ID));
		localFolder = new File(sourceConfigDao.getLocalFolder(SOURCE_ID));

		if (!localFolder.exists()) {
			System.out.println("Create new folder: " + localFolder.getPath());
			localFolder.mkdir();
		}
		ftpPath = ftpFolder.getPath() + "/" + fileName + extension;

		localPath = localFolder.getAbsolutePath() + File.separator + fileName + extension;
	}

	public boolean isDateDimExisted() {
		boolean result = false;
		try {
			System.out.println(">> Start: check datedim is existed");
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

	public boolean load() throws SQLException, IOException {
		boolean result = false;
		if (isDateDimExisted()) {
			return false;
		}
		connection = DbControlConnection.getIntance().getConnect();
		procedure = Procedure.GET_ONE_DIM;
		callStmt = connection.prepareCall(procedure);
		callStmt.setInt(1, SOURCE_ID);
		rs = callStmt.executeQuery();
		if (rs.next()) {
			int logId = rs.getInt("id");
			String path = rs.getString("ftpPath");
			try {
				connection = DbStagingControlConnection.getIntance().getConnect();
				BufferedReader br = new BufferedReader(new FileReader(new File(path)));
				String line;
				System.out.println(">> Start: load datedim into staging");
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
					callStmt.execute();
				}
				result = true;
				logDao.updateStatus(logId, "EL");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		System.out.println(">> End: " + result);
		return result;
	}

	public boolean extract(int logId) throws IOException {
		boolean result = false;
		DateTimeZone dateTimeZone = DateTimeZone.forID(TIME_ZONE);
		int count = 0;
		int date_sk = 0;
		PrintWriter pr = null;
		try {
			writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(localPath))));
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
				writer.println(output);
				writer.flush();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		if (ftpManager.pushFile(localPath, ftpFolder.getPath(), fileName + extension)) {
			logDao.updateStatus(logId, "EO");
			result = true;
			System.out.println(">> End: extract result: EO");
		} else {
			result = false;
			logDao.updateStatus(logId, "EF");
			System.out.println(">> End: extract result: EF");
		}
		return result;
	}

	public boolean execute() throws IOException {
		System.out.println(">> Start: extract source_id: " + SOURCE_ID + "\tat time: " + fileName);
		int logId = IdCreater.createIdByCurrentTime();
		logDao.insertRecord(logId, SOURCE_ID, localPath, ftpPath);
		return extract(logId);
	}

	public static void main(String[] args) throws NumberFormatException, IOException, SQLException {
//		DateDimDao dateDimDao = new DateDimDao();
//		dateDimDao.execute();
//		dateDimDao.load();
	}
}
