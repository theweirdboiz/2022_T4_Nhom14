package dao;

public interface IdCreater {
	public static String createIdByCurrentTime() {
		String currentTime = String.valueOf(System.currentTimeMillis());
		return currentTime.substring(currentTime.length() - 10, currentTime.length());
	}
}
