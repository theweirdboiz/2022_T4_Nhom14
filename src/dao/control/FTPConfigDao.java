package dao.control;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.net.ftp.FTPClient;

import com.mysql.cj.protocol.Resultset;

import dao.Query;
import db.DbControlConnection;
import db.MySQLConnection;
import model.DbHosting;
import model.FTPHosting;

public class FTPConfigDao {

	private Connection connection;
	private PreparedStatement statement;
	private String query;

	public FTPConfigDao() {
		connection = DbControlConnection.getIntance().getConnect();
	}

	public FTPHosting getFTPHosting() {
		try {
			query = Query.GET_FTP_HOSTING;
			statement = connection.prepareStatement(query);
			statement.setInt(1, 1);
			ResultSet result = statement.executeQuery();
			return result.next()
					? new FTPHosting(result.getString("host"), result.getInt("port"), result.getString("username"),
							result.getString("password"))
					: null;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {
//		FTPHosting ftpHosting = new 
		FTPConfigDao configDao = new FTPConfigDao();
//		FTPHosting ftpHosting = configDao.getFTPHosting();
//		System.out.println(ftpHosting);
	}
}
