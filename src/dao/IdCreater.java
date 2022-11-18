package dao;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public interface IdCreater {
	static final AtomicInteger ID = new AtomicInteger(0);

	public static String createIdRandom() {
		String id = UUID.randomUUID().toString();
		return id.substring(id.length() - 10, id.length());
	}

	public static int generateUniqueId() {
		return ID.incrementAndGet();
	}

}
