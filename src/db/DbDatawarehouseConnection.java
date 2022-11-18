package db;

import java.sql.Connection;

import dao.control.DbConfigDao;

public class DbDatawarehouseConnection implements IConnection{

	private MySQLConnection connection;
	private DbConfigDao configDao;

	private static DbDatawarehouseConnection instance;
	
	private DbDatawarehouseConnection() {
		configDao = new DbConfigDao();
		
		connection = new MySQLConnection(configDao.getDatawareHouseHosting());
	}
	
	public static DbDatawarehouseConnection getIntance() {
		if (instance == null) instance = new DbDatawarehouseConnection();
		return instance;
	}

	@Override
	public Connection getConnect() {
		return connection.getConnect();
	}

	@Override
	public void close() {
		connection.close();;
	}
	
	public static void main(String[] args) {
		DbDatawarehouseConnection connection = new DbDatawarehouseConnection();
	}
	
}
