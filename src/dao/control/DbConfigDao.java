package dao.control;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import dao.Query;
import db.DbControlConnection;
import model.DbHosting;

public class DbConfigDao {
	
	private Connection connection;
	private PreparedStatement statement;
	private String query;

	public DbConfigDao() {
		connection = DbControlConnection.getIntance().getConnect();
	}

	public DbHosting getStaggingHosting() {
		try {
			query = Query.GET_DB_HOSTING;
			statement = connection.prepareStatement(query);
			statement.setString(1, "stagging");
			statement.setInt(2, 1);
			ResultSet result = statement.executeQuery();
			return result.next() 
					? new DbHosting(result.getString("hosting"), result.getString("username"), result.getString("password"))
					: null;
		} catch (SQLException e) {
			return null;
		}
	}

	public DbHosting getDatawareHouseHosting() {
		try {
			query = Query.GET_DB_HOSTING;
			statement = connection.prepareStatement(query);
			statement.setString(1, "datawarehouse");
			statement.setInt(2, 1);
			ResultSet result = statement.executeQuery();
			return result.next() 
					? new DbHosting(result.getString("hosting"), result.getString("username"), result.getString("password"))
					: null;
		} catch (SQLException e) {
			return null;
		}
	}
	
	public static void main(String[] args) {
		DbConfigDao configDao = new DbConfigDao();
		System.out.println(configDao.getStaggingHosting());
	}
}
