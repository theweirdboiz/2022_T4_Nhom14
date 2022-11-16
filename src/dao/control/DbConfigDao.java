package dao.control;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import dao.Procedure;
import db.DbControlConnection;
import model.DbHosting;

public class DbConfigDao {

	private Connection connection;
	private PreparedStatement statement;
	private String query;

	public DbConfigDao() {
		connection = DbControlConnection.getIntance().getConnect();
	}

	public DbHosting getStagingHosting() {
		DbHosting dbHosting = null;
		try {
			query = Procedure.GET_DB_HOSTING;
			statement = connection.prepareCall(query);
			statement.setString(1, "staging");
			ResultSet result = statement.executeQuery();
			if (result.next()) {
				dbHosting = new DbHosting(
						result.getString("driver") + "://" + result.getString("location") + "/"
								+ result.getString("dbName"),
						result.getString("username"), result.getString("password"), result.getString("dbName"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return dbHosting;
	}

	public DbHosting getDatawareHouseHosting() {
		DbHosting dbHosting = null;
		try {
			query = Procedure.GET_DB_HOSTING;
			statement = connection.prepareCall(query);
			statement.setString(1, "warehouse");
			ResultSet result = statement.executeQuery();
			if (result.next()) {
				dbHosting = new DbHosting(
						result.getString("driver") + "://" + result.getString("location") + "/"
								+ result.getString("dbName"),
						result.getString("username"), result.getString("password"), result.getString("dbName"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return dbHosting;
	}

	public static void main(String[] args) {
		DbConfigDao configDao = new DbConfigDao();
		System.out.println(configDao.getDatawareHouseHosting());
	}
}
