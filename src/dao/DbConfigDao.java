package dao;

import db.MySQLConnection;
import model.DbHosting;

public class DbConfigDao {

	public DbHosting getStaggingHosting() {
		return null;
	}

	public DbHosting getDatawareHouseHosting() {
		return null;
	}

	public DbConfigDao() {
		MySQLConnection mySqlConnection = new MySQLConnection("jdbc:mysql://localhost:3306/staging", "root", "");
	}

	public static void main(String[] args) {
		DbConfigDao test = new DbConfigDao();

	}
}
