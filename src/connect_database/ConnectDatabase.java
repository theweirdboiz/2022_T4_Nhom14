package connect_database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectDatabase {
    public Connection conn = null;
    String DATABASE_DRIVER = "com.mysql.cj.jdbc.Driver";
    String url = "jdbc:mysql://localhost:3306/";
    String username = "kinethuc";
    String password = "password";

    public void connecting(String databaseName) throws ClassNotFoundException, SQLException {
        System.out.println("Connecting database...");
        try {
            Class.forName(DATABASE_DRIVER);
            url = url + databaseName;
            conn = DriverManager.getConnection(url, username, password);

//			while()

//			String loadQuery = "LOAD DATA INFILE 'C:\\\\Users\\\\kient\\\\eclipse-workspace\\\\data-warehouse\\\\data.csv' INTO TABLE staging FIELDS TERMINATED BY ',' IGNORE 1 LINES";
//			System.out.println(loadQuery);
//			Statement stmt = conn.createStatement();
//			stmt.execute(loadQuery);
            System.out.println("Database connected!");


        } catch (Exception exception) {
            System.err.println(exception.getMessage());
        }
    }

    public Connection getConn() {
        return conn;
    }

    public void closeConnecting() throws SQLException {
        conn.close();
    }
}
