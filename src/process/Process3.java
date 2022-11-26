package process;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import dao.IdCreater;
import dao.control.LogControllerDao;
import dao.staging.DateDimDao;
import dao.staging.ProvinceDimDao;
import dao.staging.TimeDimdao;
import dao.staging.WeatherFactDao;
import dao.warehouse.WarehouseDao;

public class Process3 {
	private LogControllerDao logControllerDao;

	private DateDimDao dateDimDaoStaging;
	private TimeDimdao timeDimdaoStaging;
	private ProvinceDimDao provinceDimDaoStaging;
	private WeatherFactDao weatherFactDaoStaging;

	private dao.warehouse.DateDimDao dateDimDaoDatawarehouse;
	private dao.warehouse.TimeDimdao timDimDaoDatawarehouse;
	private dao.warehouse.ProvinceDimDao provinceDimDaoDatawarehouse;
	private dao.warehouse.WeatherFactDao weatherFaceDaoDatawarehouse;

	public Process3() {
//		warehouseDao = new WarehouseDao();
		logControllerDao = new LogControllerDao();

		dateDimDaoStaging = new DateDimDao();
		timeDimdaoStaging = new TimeDimdao();
		provinceDimDaoStaging = new ProvinceDimDao();
		weatherFactDaoStaging = new WeatherFactDao();

		dateDimDaoDatawarehouse = new dao.warehouse.DateDimDao();
		timDimDaoDatawarehouse = new dao.warehouse.TimeDimdao();
		provinceDimDaoDatawarehouse = new dao.warehouse.ProvinceDimDao();
		weatherFaceDaoDatawarehouse = new dao.warehouse.WeatherFactDao();

	}

	public void loadProvinceDim() throws SQLException {
		ResultSet rs = provinceDimDaoStaging.getAll();
		try {
			while (rs.next()) {
				provinceDimDaoDatawarehouse.insert(IdCreater.createIdByCurrentTime(), rs.getInt("id"),
						rs.getString("name"));
			}
			System.out.println("Load provincedim to warehouse successful!");
		} catch (

		SQLException e) {
			System.out.println("Load provincedim to warehouse not successful!");
			e.printStackTrace();
		}
	}

	public void loadTimeDim() {

	}

	public void loadDateDim() {

	}

	public void loadWeatherFact() {

	}

	public void execute() throws SQLException {
		loadDateDim();
		loadTimeDim();
		// kiem tra co su thay doi cua provincedim - slowly change
		loadProvinceDim();
		loadWeatherFact();
	}

	public static void main(String[] args) throws SQLException {
		Process3 thirdProcessing = new Process3();
		thirdProcessing.loadProvinceDim();
	}
}
