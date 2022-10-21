package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySQLConnection implements IConnection {

	private static final String DATABASE_DRIVER = "com.mysql.cj.jdbc.Driver";

	private Connection connection;

	public MySQLConnection(String url, String username, String password) {
		try {
			System.out.println("Connecting...");
			Class.forName(DATABASE_DRIVER);
			connection = DriverManager.getConnection(url, username, password);
			System.out.println("Connected!");

		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Connection getConnect() {
		return this.connection;
	}

	@Override
	public void close() {
		try {
			this.connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
