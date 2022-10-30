package dao;

import java.util.concurrent.atomic.AtomicInteger;

public interface IdCreater {
	static final AtomicInteger ID = new AtomicInteger(0);

	public static String createIdByCurrentTime() {
		String currentTime = String.valueOf(System.currentTimeMillis());
		return currentTime.substring(currentTime.length() - 10, currentTime.length());
	}

	public static int generateUniqueId() {
		return ID.incrementAndGet();
	}

}
