package dao;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public interface CurrentTimeStamp {
	DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-mm-dd_hh-mm-ss");
	LocalDateTime now = LocalDateTime.now();

	public static String getCurrentTimeStamp() {
		return dtf.format(now);
	}
}
