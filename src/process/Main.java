package process;

import java.io.IOException;
import java.sql.SQLException;

import db.MySQLConnection;

public class Main {
	public Main() {
	}

	public static void main(String[] args) {
		Main main = new Main();
		MySQLConnection mySQLConnection = new MySQLConnection("jdbc:mysql://localhost:3306/control", "root", null);
		FirstProcessingThoiTietVn firstProcessingThoiTietVn = new FirstProcessingThoiTietVn();
		try {
			firstProcessingThoiTietVn.runScript();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
