package dao;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public interface CreateDateDim {
//	public static final String OUT_FILE = "date_dim.csv";
//	public static final int NUMBER_OF_RECORD = 5432;
//	public static final String TIME_ZONE = "PST8PDT";
//	public File file = new File(OUT_FILE);
//
//	public static boolean create() {
//		DateTimeZone dateTimeZone = DateTimeZone.forID(TIME_ZONE);
//		int count = 0;
//		int date_sk = 0;
//		PrintWriter pr = null;
//		try {
//			if (file.exists()) {
//				file.delete();
//			}
//			pr = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		DateTime startDateTime = new DateTime(2022, 10, 31, 0, 0, 0);
//		while (count <= NUMBER_OF_RECORD) {
//			startDateTime = startDateTime.plus(Period.days(1));
//			Date startDate = startDateTime.toDate();
//			Calendar calendar = Calendar.getInstance();
//			calendar.setTime(startDate);
//
//			// Date_SK
//			date_sk += 1; // 1
//			SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd");
//			// Full Date
//			String full_date = dt.format(calendar.getTime()); // 2
//			dt = new SimpleDateFormat("yyyy");
//			// Calendar Year
//			String calendar_year = dt.format(calendar.getTime()); // 7
//
//			// Calendar Month
////			String calendar_month = calendar.getDisplayName(Calendar.MONTH, Calendar.LONG, Locale.US); // 6
//
//			int calendar_month = calendar.MONTH;
//
//			int calendar_day_of_month = calendar.get(Calendar.DAY_OF_MONTH); // 9
//			// Calendar day_of_week
//			String calendar_day_of_week = calendar.getDisplayName(Calendar.DAY_OF_WEEK, Calendar.LONG, Locale.US); // 5
//
//			String output = date_sk + "," + full_date + "," + calendar_year + "," + calendar_month + ", "
//					+ calendar_day_of_month + ", " + calendar_day_of_week;
////			System.out.println(output);
//			count++;
//			// Printout Data to File
//			pr.println(output);
//			pr.flush();
//		}
//		return file.length() > 0;
//	}
}
