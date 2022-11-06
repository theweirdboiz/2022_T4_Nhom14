package dao;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

public interface CurrentTimeStamp {
	Timestamp timestamp = new Timestamp(System.currentTimeMillis());
	SimpleDateFormat dtf = new SimpleDateFormat("yyyy-MM-dd_HH");

	public static String getCurrentTimeStamp() {
		return dtf.format(timestamp);
	}

	public static String getCurrentDate() {
		SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = Calendar.getInstance();
		String currentDate = dt.format(calendar.getTime());
		return currentDate;
	}

	public static String getCurrentDate(String date, String hour) {
		return date + "_" + hour;
	}

	public static void main(String[] args) {
		System.out.println(CurrentTimeStamp.getCurrentTimeStamp());
	}
}
