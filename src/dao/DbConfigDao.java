package dao;

<<<<<<< HEAD
import model.DbHosting;

public class DbConfigDao {

	public DbHosting getStaggingHosting() {
		return null;
	}
	
	public DbHosting getDatawareHouseHosting() {
		return null;
=======
import db.MySQLConnection;

public class DbConfigDao {
	
	public DbConfigDao() {
		MySQLConnection mySqlConnection = new MySQLConnection("jdbc:mysql://localhost:3306/staging", "root", "");
	}
	public static void main(String[] args) {
		DbConfigDao test = new DbConfigDao();
>>>>>>> 921ac9c47d22e8306f19e9004e4f37ce73ab21ab
	}
}
