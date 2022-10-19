package db;

import java.sql.Connection;

public interface IConnection {
	public Connection getConnect();
	public void close();
}
