package db;

import java.sql.Connection;

import dao.control.DbConfigDao;

public class DbWarehouseConnection {
	private MySQLConnection connection;
	private DbConfigDao configDao;

	private static DbWarehouseConnection instance;

	private DbWarehouseConnection() {
		configDao = new DbConfigDao();
		connection = new MySQLConnection(configDao.getDatawareHouseHosting());
	}

	public static DbWarehouseConnection getIntance() {
		if (instance == null)
			instance = new DbWarehouseConnection();
		return instance;
	}

	public Connection getConnect() {
		return connection.getConnect();
	}

	public void close() {
		connection.close();
	}

	public static void main(String[] args) {
		DbWarehouseConnection db = new DbWarehouseConnection();
		db.getIntance().getConnect();
	}
}
