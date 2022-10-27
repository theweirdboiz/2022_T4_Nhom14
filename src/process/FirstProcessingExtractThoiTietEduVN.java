package process;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.FileDescriptor;

import dao.IdCreater;
import dao.control.LogControllerDao;
import dao.control.SourceConfigDao;
import ftp.FTPManager;

public class FirstProcessingExtractThoiTietEduVN {
	private SourceConfigDao sourceConfigDao;
	private LogControllerDao log;
	private FTPManager ftpManager;
	
	private static final String SOURCE_ID = "2";
	private String source;
	private String fileName;
	private String path;
	
	public FirstProcessingExtractThoiTietEduVN() {
		sourceConfigDao = new SourceConfigDao();
		source = sourceConfigDao.getURL(SOURCE_ID);
		fileName = sourceConfigDao.getFileName(SOURCE_ID);
		
		log = new LogControllerDao();
		ftpManager = new FTPManager();
	}
	
	public void excute()  {
		System.out.println("Extracting...");
		log.insertLogDefault(IdCreater.createIdByCurrentTime(), SOURCE_ID);
		try {
			Date date = new Date(Calendar.getInstance().getTime().getTime() + 1900);
			DateFormat dateFormatForFileName = new SimpleDateFormat("dd-mm-yyyy_hh-mm-ss");
			fileName = dateFormatForFileName.format(date);
			File folderExtract = new File(sourceConfigDao.getPathFolder(SOURCE_ID));
			if (!folderExtract.exists()) folderExtract.mkdir();
			path = folderExtract.getAbsolutePath() + File.separator + fileName;
			PrintWriter writer = new PrintWriter(new File(path));
			Document root = Jsoup.connect(source).get();
			
			Elements provincesHTML = root.select("#child-item-childrens a");
			DateFormat dateFormat = new SimpleDateFormat("dd/mm/yyyy hh:mm:ss");
			writer.write(dateFormat.format(date) + "\n");
			
			for(Element elm : provincesHTML) {
				String id = IdCreater.createIdByCurrentTime();
				Document weatherEachProvince = Jsoup.connect(source + elm.attr("href")).get();
				String province = elm.text();
				String currentTemperature = weatherEachProvince.selectFirst(".current-temperature").text();
				String overview = weatherEachProvince.select(".overview-caption-item-detail").get(0).text();
				Elements weatherDetails = weatherEachProvince.select(".weather-detail-location");
				String lostestTemperature = weatherDetails.get(0).select("span").get(1).text().split("/")[0];
				String highestTemperature = weatherDetails.get(0).select("span").get(1).text().split("/")[1];
				String humidity = weatherDetails.get(1).select("span").get(2).text();
				String vision = weatherDetails.get(2).select("span").get(1).text();
				String wind = weatherDetails.get(3).select("span").get(1).text();
				String stopPoint = weatherDetails.get(4).select("span").get(1).text();
				Float uv = Float.parseFloat(weatherDetails.get(5).select("span").get(1).text());
				String airQuality = weatherEachProvince.select(".air-api").text();
			
				int currentTemperatureNum = Integer.parseInt(currentTemperature.substring(0, currentTemperature.length() - 1).trim());
				int lostestTemperatureNum = Integer.parseInt(lostestTemperature.substring(0, lostestTemperature.length() - 1).trim());
				int highestTemperatureNum = Integer.parseInt(highestTemperature.substring(0, highestTemperature.length() - 1).trim());
				float humidityFloat = Float.parseFloat(humidity.split(" ")[0]) / 100.0f;
				float visionNum = Float.parseFloat(vision.split(" ")[0]);
				float windFloat = Float.parseFloat(wind.split(" ")[0]);
				int stopPointNum = Integer.parseInt(stopPoint.split(" ")[0]);
				
				writer.write(id + "," + province + ","
						+ currentTemperatureNum + "," + lostestTemperatureNum + "," + highestTemperatureNum + ","
						+ humidityFloat + "," + overview + ","
						+ visionNum + "," + windFloat + "," + stopPointNum + "," + uv + "," + airQuality + "\n");
			}
			writer.flush();
			writer.close();
			
			if (ftpManager.pushFile(path, sourceConfigDao.getDistFolder(SOURCE_ID), fileName)) {
				log.setStatus(SOURCE_ID, "EO");
				System.out.println("Extract OK");
			}else {
				log.setStatus(SOURCE_ID, "EF");
				System.out.println("Extract Fail");
			}
		} catch (IOException e) {
			log.setStatus(SOURCE_ID, "EF");
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
		FirstProcessingExtractThoiTietEduVN processing = new FirstProcessingExtractThoiTietEduVN();
		processing.excute();
	}
}
