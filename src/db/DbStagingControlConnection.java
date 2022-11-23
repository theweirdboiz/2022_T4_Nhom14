package db;

import java.sql.Connection;

import dao.control.DbConfigDao;

public class DbStagingControlConnection {
	private MySQLConnection connection;
	private DbConfigDao configDao;

	private static DbStagingControlConnection instance;

	private DbStagingControlConnection() {
		configDao = new DbConfigDao();
		connection = new MySQLConnection(configDao.getStagingHosting());
	}

	public static DbStagingControlConnection getIntance() {
		if (instance == null)
			instance = new DbStagingControlConnection();
		return instance;
	}

	public Connection getConnect() {
		return connection.getConnect();
	}

	public void close() {
		connection.close();
	}
}
