package process;

import db.MySQLConnection;

public class Main {
	public static void main(String[] args) {
		MySQLConnection mySQLConnection = new MySQLConnection("jdbc:mysql://localhost:3306/control", "root", null);
		
	}
}
