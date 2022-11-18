package db;

import java.sql.Connection;

import dao.control.DbConfigDao;

public class DbStaggingConnection implements IConnection{
	private MySQLConnection connection;
	private DbConfigDao configDao;

	private static DbStaggingConnection instance;
	
	private DbStaggingConnection() {
		configDao = new DbConfigDao();
		
		connection = new MySQLConnection(configDao.getStaggingHosting());
	}
	
	public static DbStaggingConnection getIntance() {
		if (instance == null) instance = new DbStaggingConnection();
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
		DbStaggingConnection dbStaggingConnection = new DbStaggingConnection();
	}
}
