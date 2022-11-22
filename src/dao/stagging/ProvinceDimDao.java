package dao.stagging;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import dao.Query;
import db.DbStaggingConnection;

public class ProvinceDimDao {

	private Connection connection;
	private PreparedStatement statement;
	private String query;

	public ProvinceDimDao() {
		connection = DbStaggingConnection.getIntance().getConnect();
	}
	
	public boolean insert(String data) {
		String[] elms = data.split(",");
		
		query = Query.INSERT_DATA_TO_PROVICE;
		try {
			statement = connection.prepareStatement(query);
			statement.setString(1, elms[0]);
			statement.setString(2, elms[1]);
			return statement.executeUpdate() > 0;
		} catch (SQLException e) {
			return false;
		}
	}
	
	public String getIdByProvinceName(String provinceName) {
		query = Query.GET_ID_BY_PROVINCE_NAME_STAGGING;
		try {
			statement = connection.prepareStatement(query);
			statement.setString(1, provinceName);
			ResultSet resultSet = statement.executeQuery();
			return resultSet.next() ? resultSet.getString("id") : null;
		} catch (SQLException e) {
			return null;
		}
	}
	
	public String getProvinceNameById(String id) {
		query = Query.GET_PROVINCE_NAME_BY_ID_STAGGING;
		
		try {
			statement = connection.prepareStatement(query);
			statement.setString(1, id);
			ResultSet rs = statement.executeQuery();
			return rs.next() ? rs.getString("name_province") : null;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public ResultSet getData() {
		query = Query.GET_DATA_FROM_PROVINCE_DIM_STAGGING;
		
		try {
			statement = connection.prepareStatement(query);
			return statement.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public boolean deleteAll(){
		query = Query.DELETE_ALL_PROVINCEDIM_STAGGING;
		
		try {
			statement = connection.prepareStatement(query);
			return statement.executeUpdate() > 0;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public static void main(String[] args) {
		ProvinceDimDao provinceDimDao = new ProvinceDimDao();
		System.out.println(provinceDimDao.getIdByProvinceName("Phú Thọ"));
	}
}
