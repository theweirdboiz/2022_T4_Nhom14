package dao.stagging;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.util.Locale;
import java.util.UUID;

import dao.IdCreater;
import dao.Query;
import db.DbStaggingConnection;

public class DateDimDao {

	private Connection connection;
	private PreparedStatement statement;
	private String query;

	public DateDimDao() {
		connection = DbStaggingConnection.getIntance().getConnect();
	}

	public boolean insert(String id,String dateString) {
        DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
        LocalDateTime localDate = LocalDateTime.parse(dateString, formatter1);
		try {
			query = Query.INSERT_DATA_TO_DATE_DIM_STAGGING;
			statement = connection.prepareStatement(query);
			statement.setString(1, id);
			statement.setInt(2, localDate.getDayOfMonth());
			statement.setInt(3, localDate.getMonthValue());
			statement.setInt(4, localDate.getYear());
			statement.setInt(5, localDate.getHour());
			statement.setInt(6, localDate.getMinute());
			statement.setInt(7, localDate.getSecond());
			statement.setString(8, localDate.getDayOfWeek().toString());
			return statement.executeUpdate() > 0;
		} catch (SQLException e) {
			return false;
		}
	}
	
	public ResultSet getDataDateDim() {
		query = Query.GET_DATA_FROM_DATE_DIM_STAGGING;
		
		try {
			statement = connection.prepareStatement(query);
			return statement.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public boolean deleteAll(){
		query = Query.DELETE_ALL_DATEDIM_STAGGING;
		
		try {
			statement = connection.prepareStatement(query);
			return statement.executeUpdate() > 0;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public static void main(String[] args) {
	}
}
