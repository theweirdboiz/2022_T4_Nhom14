package dao;

import db.MySQLConnection;

public class DbConfigDao {
	
	public DbConfigDao() {
		MySQLConnection mySqlConnection = new MySQLConnection("jdbc:mysql://localhost:3306/staging", "root", "");
	}
	public static void main(String[] args) {
		DbConfigDao test = new DbConfigDao();
	}
}
