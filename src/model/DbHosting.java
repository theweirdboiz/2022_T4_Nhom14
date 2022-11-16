package model;

public class DbHosting {
	private String url;
	private String userName;
	private String password;
	private String type;

	public DbHosting(String url, String userName, String password, String type) {
		this.url = url;
		this.userName = userName;
		this.password = password;
		this.type = type;
	}

	public String getType() {
		return type;
	}

	@Override
	public String toString() {
		return "DbHosting [url=" + url + ", userName=" + userName + ", password=" + password + ", type=" + type + "]";
	}

	public String getUrl() {
		return url;
	}

	public String getUserName() {
		return userName;
	}

	public String getPassword() {
		return password;
	}
}
