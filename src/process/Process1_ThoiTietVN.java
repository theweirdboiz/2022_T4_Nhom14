package process;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import dao.CurrentTimeStamp;
import dao.IdCreater;
import dao.control.LogControllerDao;
import dao.control.SourceConfigDao;

import ftp.FTPManager;

public class Process1_ThoiTietVN {

	private FTPManager ftpManager;
	private LogControllerDao log;

	private SourceConfigDao sourceConfigDao;
	private static final int SOURCE_ID = 1;

	private String sourceUrl, ftpPath, localPath, rawFtpPath, rawLocalPath;
	private String fileName, rawFileName, extension, separator;

	private PrintWriter writer, rawWriter;
	private Date currentDate;
	private File localFolder, ftpFolder;

	public Process1_ThoiTietVN() {
		sourceConfigDao = new SourceConfigDao();
		sourceUrl = sourceConfigDao.getURL(SOURCE_ID);

		log = new LogControllerDao();
		ftpManager = new FTPManager(SOURCE_ID);

		extension = ".csv";
		separator = ", ";

		fileName = CurrentTimeStamp.getCurrentTimeStamp();
		rawFileName = "raw_" + CurrentTimeStamp.getCurrentTimeStamp();

		ftpFolder = new File(sourceConfigDao.getFtpFolder(SOURCE_ID));
		localFolder = new File(sourceConfigDao.getLocalFolder(SOURCE_ID));
		if (!localFolder.exists()) {
			System.out.println("Create new folder: " + localFolder.getName());
			localFolder.mkdirs();
		}

		ftpPath = ftpFolder.getPath() + "/" + fileName + extension;
		localPath = localFolder.getAbsolutePath() + File.separator + fileName + extension;

		rawFtpPath = ftpFolder.getPath() + "/" + fileName + extension;
		rawLocalPath = localFolder.getAbsolutePath() + File.separator + "raw_" + fileName + extension;
	}

	public void execute() throws IOException {
		System.out.println(">> Start: extract source_id: " + SOURCE_ID + "\tat time: " + fileName);
		int logId = IdCreater.createIdByCurrentTime();
		log.insertRecord(logId, SOURCE_ID, localPath, ftpPath);
		try {
			extract(logId);
			log.updateStatus(logId, "EO");
		} catch (SQLException e) {
			log.updateStatus(logId, "EF");
			e.printStackTrace();
		} catch (IOException e) {
			log.updateStatus(logId, "EF");
		}
	}

	private boolean extract(int logId) throws SQLException, IOException {
		boolean result = false;
		// 2.2 Tiến hành extract
		try {
			// Mở file
			writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(localPath))));
			rawWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(rawLocalPath))));

			SimpleDateFormat dt = new SimpleDateFormat("dd-MM-yy HH:mm");
			String currentTimeStamp = dt.format(CurrentTimeStamp.timestamp);

			Document doc = Jsoup.connect(sourceUrl).get();
			Elements provinces = doc.select(".megamenu a");
			for (int i = 0; i < provinces.size(); i++) {
				int id = IdCreater.generateUniqueId();
				String dataURL = sourceUrl + provinces.get(i).attr("href");
				Document docItem = Jsoup.connect(dataURL).get();
				// province
				String provinceName = provinces.get(i).attr("title");

				Element currentTemp = docItem.select(".current-temperature").first();
				// current_temperature
				String currentTemperatureText = currentTemp.text();
				// overview
				String overViewText = docItem.select(".overview-caption-item.overview-caption-item-detail").text();
				// lowest_temp
				String lowestTempText = docItem.select(".text-white.op-8.fw-bold:first-of-type").text().split("/")[0];
				// maximum_temp
				String maximumText = docItem.selectFirst(".weather-detail .text-white.op-8.fw-bold:first-child").text()
						.split("/")[1];
				// humidity
				String humidityText = docItem.select(".weather-detail .text-white.op-8.fw-bold").get(1).text();
				// vision
				String visionText = docItem.select(".weather-detail .text-white.op-8.fw-bold").get(2).text();
				// wind
				String windText = docItem.select(".weather-detail .text-white.op-8.fw-bold").get(3).text();
				// stop_point
				String stopPointText = docItem.select(".weather-detail .text-white.op-8.fw-bold").get(4).text();
				// uv_index
				String uvIndexText = docItem.select(".weather-detail .text-white.op-8.fw-bold").get(5).text();
				// air_quality
				String airQualityText = docItem.select(".air-api.air-active").text();

				rawWriter.println(id + separator + provinceName + separator + currentTimeStamp + separator
						+ currentTemperatureText + separator + lowestTempText + separator + maximumText + separator
						+ humidityText + separator + overViewText + separator + windText + separator + visionText
						+ separator + stopPointText + separator + uvIndexText + separator + airQualityText);
				// pretreatment
				int currentTemperatureNum = Integer
						.parseInt(currentTemperatureText.substring(0, currentTemperatureText.length() - 1).trim());
				int lowestTemperatureNum = Integer
						.parseInt(lowestTempText.substring(0, lowestTempText.length() - 1).trim());
				int maximumTemperatureNum = Integer.parseInt(maximumText.substring(0, maximumText.length() - 1).trim());
				float humidityFloat = Float.parseFloat(humidityText.split("%")[0]) / 100.0f;
				float visionNum = Float.parseFloat(visionText.split(" ")[0]);
				float windFloat = Float.parseFloat(windText.split(" ")[0]);
				int stopPointNum = Integer.parseInt(stopPointText.split(" ")[0]);
				Float uvIndexFloat = Float.parseFloat(uvIndexText);

				writer.println(id + separator + provinceName + separator + currentTimeStamp + separator
						+ currentTemperatureNum + separator + overViewText + separator + lowestTemperatureNum
						+ separator + maximumTemperatureNum + separator + humidityFloat + separator + visionNum
						+ separator + windFloat + separator + stopPointNum + separator + uvIndexFloat + separator
						+ airQualityText);
			}
			rawWriter.flush();
			writer.flush();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		if (ftpManager.pushFile(localPath, ftpFolder.getPath().replace('\\', '/'), fileName + extension)
				&& ftpManager.pushFile(rawLocalPath, ftpFolder.getPath().replace("\\", "/"), rawFileName + extension)) {
			log.updateStatus(logId, "EO");
			result = true;
			System.out.println(">> End: extract result: EO");
		} else {
			log.updateStatus(logId, "EF");
			result = false;
			System.out.println(">> End: extract result: EF");
		}
		return result;
	}

	public static void main(String[] args) throws IOException, SQLException {
		Process1_ThoiTietVN firstProcessing = new Process1_ThoiTietVN();
		firstProcessing.execute();
	}
}