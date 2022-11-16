package db;

import java.sql.Connection;

public class DbControlConnection implements IConnection {

	private MySQLConnection connection;
	private static final String DB_CONTROL_URL = "jdbc:mysql://localhost:3306/control";
	private static final String USERNAME = "kinethuc";
	private static final String PASSWORD = "password";

	private static DbControlConnection instance;

	private DbControlConnection() {
		connection = new MySQLConnection(DB_CONTROL_URL, USERNAME, PASSWORD);
	}

	public static DbControlConnection getIntance() {
		if (instance == null)
			instance = new DbControlConnection();
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
