package process;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import dao.IdCreater;
import dao.control.LogControllerDao;
import dao.stagging.DateDimDao;
import dao.stagging.ProvinceDimDao;
import dao.stagging.WeatherFactDao;

public class ThirdProcessing {

	//control
	private LogControllerDao logControllerDao;

	//stagging
	private DateDimDao dateDimDaoStagging;
	private ProvinceDimDao provinceDimDaoStagging;
	private WeatherFactDao weatherFactDaoStagging;
	
	//datawarehouse
	private dao.datawarehouse.DateDimDao dateDimDaoDatawarehouse;
	private dao.datawarehouse.ProvinceDimDao provinceDimDaoDatawarehouse;
	private dao.datawarehouse.WeatherFactDao weatherFaceDaoDatawarehouse;
	
	public ThirdProcessing() {
		logControllerDao = new LogControllerDao();
		
		dateDimDaoStagging = new DateDimDao();
		provinceDimDaoStagging = new ProvinceDimDao();
		weatherFactDaoStagging = new WeatherFactDao();
		
		dateDimDaoDatawarehouse = new dao.datawarehouse.DateDimDao();
		provinceDimDaoDatawarehouse = new dao.datawarehouse.ProvinceDimDao();
		weatherFaceDaoDatawarehouse = new dao.datawarehouse.WeatherFactDao();
	}
	
	public void execute() {
		loadProvinceSource();
		loadWeatherSources();
		if (weatherFactDaoStagging.deleteAll()) System.out.println("Deleted all weather fact in stagging");
		else System.out.println("Error");
		if (dateDimDaoStagging.deleteAll()) System.out.println("Deleted all date dim in stagging");
		else System.out.println("Error");
		if (provinceDimDaoStagging.deleteAll()) System.out.println("Deleted all province dim in stagging");
		else System.out.println("Error");
	}
	
	public void loadProvinceSource() {
		ResultSet logProvinceRs = logControllerDao.getLogHasProvinceDimWithELStatus();
		List<String> logIds = new ArrayList<>();
		try {
			while(logProvinceRs.next()) logIds.add(logProvinceRs.getString("id"));
			if (logIds.size() > 0) {
				loadProvinceDim();
				logIds.forEach(logId -> {logControllerDao.setStatus(logId, "ELDW");});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void loadWeatherSources() {
		ResultSet logWeatherRs = logControllerDao.getLogHasWeatherWithELStatus();
		List<String> logIds = new ArrayList<>();
		try {
			while(logWeatherRs.next()) logIds.add(logWeatherRs.getString("id"));
			if (logIds.size() > 0) {
				loadDateDim();
				loadWeatherFact();
				logIds.forEach(logId -> {logControllerDao.setStatus(logId, "ELDW");});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void loadDateDim() {
		ResultSet dateDimRs = dateDimDaoStagging.getDataDateDim();
		try {
			while(dateDimRs.next()) 
				if 
				(
						dateDimDaoDatawarehouse
						.insert
						(
								dateDimRs.getString("id"), 
								dateDimRs.getInt("date"), 
								dateDimRs.getInt("month"), 
								dateDimRs.getInt("year"), 
								dateDimRs.getInt("hour"), 
								dateDimRs.getInt("minute"), 
								dateDimRs.getInt("second"), 
								dateDimRs.getString("dayOfWeek")
								)
				) System.out.println("Inserted 1 row to datawarehouse date dim");
				else System.out.println("Error");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void loadProvinceDim() {
		ResultSet provinceStaggingRs = provinceDimDaoStagging.getData();
		
		try {
			while(provinceStaggingRs.next()) {
				String id = provinceStaggingRs.getString("id");
				String provinceName = provinceStaggingRs.getString("nameProvince");
				
				boolean exsistProvinceName = provinceDimDaoDatawarehouse.getIdByProvinceName(provinceName) != null;
				
				if (!exsistProvinceName) {
					boolean inserted = provinceDimDaoDatawarehouse.insert(id, provinceName);
					
					if (inserted) System.out.println("Inserted 1 row to province dim datawarehouse");
					else System.out.println("Error");
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void loadWeatherFact() {
		if (weatherFaceDaoDatawarehouse.setTimeExpried()) System.out.println("Updated time expried privous");
		else System.out.println("Error");
		ResultSet weatherFactRs = weatherFactDaoStagging.getData();
		try {
			while(weatherFactRs.next()) {
				String sk = IdCreater.createIdRandom();
				String naturalKey = weatherFactRs.getString("id");
				String provinceId = provinceDimDaoDatawarehouse
						.getIdByProvinceName(provinceDimDaoStagging
								.getProvinceNameById(weatherFactRs.getString("provinceId")));
				String dateId = weatherFactRs.getString("dateId");
				int currentTemperature = weatherFactRs.getInt("currentTemperature");
				int lowestTemperature = weatherFactRs.getInt("lowestTemperature");
				int highestTemperature = weatherFactRs.getInt("highestTemperature");
				float humidity = weatherFactRs.getFloat("humidity");
				String overview = weatherFactRs.getString("overView");
				float wind = weatherFactRs.getFloat("wind");
				float vision = weatherFactRs.getFloat("vision");
				int stopPoint = weatherFactRs.getInt("stopPoint");
				float uvIndex = weatherFactRs.getFloat("uvIndex");
				String airQuality = weatherFactRs.getString("airQuality");

				boolean inserted = weatherFaceDaoDatawarehouse
						.insert(sk, naturalKey, provinceId, dateId, currentTemperature, 
								lowestTemperature, highestTemperature, humidity, overview, 
								wind, vision, stopPoint, uvIndex, airQuality);
				
				if(inserted) System.out.println("Inserted 1 row to weather fact datawarehouse");
				else System.out.println("Error");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		ThirdProcessing processing = new ThirdProcessing();
		processing.execute();
	}
}
