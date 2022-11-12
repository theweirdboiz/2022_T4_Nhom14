package dao;

import java.util.concurrent.atomic.AtomicInteger;

public interface IdCreater {
	static final AtomicInteger ID = new AtomicInteger(0);

	public static int createIdByCurrentTime() {
		String currentTime = String.valueOf(System.currentTimeMillis());
		return Integer.parseInt(currentTime.substring(currentTime.length() - 8, currentTime.length()));
	}

	public static int generateUniqueId() {
		return ID.incrementAndGet();
	}

}
