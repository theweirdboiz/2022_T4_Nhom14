package db;

import java.sql.Connection;

public class DbStagingConnection implements IConnection {
	private MySQLConnection connection;

	private static DbStagingConnection instance;

	private String DB_CONTROL_URL;
	private String USERNAME;
	private String PASSWORD;

	private DbStagingConnection(String DB_CONTROL_URL, String USERNAME, String PASSWORD) {
		this.DB_CONTROL_URL = DB_CONTROL_URL;
		this.USERNAME = USERNAME;
		this.PASSWORD = PASSWORD;

		connection = new MySQLConnection(this.DB_CONTROL_URL, this.USERNAME, this.PASSWORD);
	}

	public static DbStagingConnection getIntance() {
		if (instance == null)
			instance = new DbStagingConnection(instance.DB_CONTROL_URL, instance.USERNAME, instance.PASSWORD);
		return instance;
	}

	@Override
	public Connection getConnect() {
		return connection.getConnect();
	}

	@Override
	public void close() {
		connection.close();
		;
	}

}
