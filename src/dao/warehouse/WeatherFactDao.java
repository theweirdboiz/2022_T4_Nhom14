package dao.warehouse;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import dao.Procedure;
import db.DbStagingControlConnection;

public class WeatherFactDao {
	private String procedure;
	private CallableStatement callStmt;
	private PreparedStatement ps;
	private ResultSet rs;
	Connection connection;

	public WeatherFactDao() {
		connection = DbStagingControlConnection.getIntance().getConnect();
		
	}

	
	public static void main(String[] args) {
		WeatherFactDao weatherFactDao = new WeatherFactDao();
	}

}
