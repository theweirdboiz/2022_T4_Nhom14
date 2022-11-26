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
import java.util.Date;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import dao.CurrentTimeStamp;
import dao.IdCreater;
import dao.Procedure;
import dao.control.LogControllerDao;
import dao.control.SourceConfigDao;
import db.DbControlConnection;
import ftp.FTPManager;

public class Process1_ThoiTietEduVN {
	private FTPManager ftpManager;
	private LogControllerDao log;

	private SourceConfigDao sourceConfigDao;
	private static final int SOURCE_ID = 2;

	private String sourceUrl, destinationUrl;
	private String fileName, rawFileName, extension, separator;
	private String path, rawPath;

	private PrintWriter writer, rawWriter;
	private Date currentDate;

	public Process1_ThoiTietEduVN() {
		sourceConfigDao = new SourceConfigDao();
		sourceUrl = sourceConfigDao.getURL(SOURCE_ID);

		log = new LogControllerDao();
		ftpManager = new FTPManager(SOURCE_ID);

		DateFormat dateFormatForFileName = new SimpleDateFormat("dd-MM-yyyy_HH");
		currentDate = new Date();
		fileName = dateFormatForFileName.format(currentDate);
		rawFileName = "raw_" + dateFormatForFileName.format(currentDate);

		extension = ".csv";
		separator = ", ";

		File folderExtract = new File(sourceConfigDao.getPathFolder(SOURCE_ID) + "/" + fileName);

		if (!folderExtract.exists()) {
			System.out.println("Create new folder: " + folderExtract.getName());
			folderExtract.mkdirs();
		}
		path = folderExtract.getAbsolutePath() + File.separator + fileName + extension;
		rawPath = folderExtract.getAbsolutePath() + File.separator + rawFileName + extension;

		destinationUrl = sourceConfigDao.getDistFolder(SOURCE_ID) + "/" + fileName + "/" + fileName + extension;

	}

	public void execute() throws IOException {
		System.out.println("Extracting source id: " + SOURCE_ID + "\tat time: " + fileName);
		int logId = IdCreater.createIdByCurrentTime();
		String status = log.getFileStatus(SOURCE_ID);
		switch (status) {
		case "EO":
			System.out.println("Result extract: This source has been extracted");
			break;
		case "ER", "EF":
			try {
				extract(logId);
			} catch (SQLException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			break;
		default:
			try {
				log.insertRecord(logId, SOURCE_ID, destinationUrl);
				extract(logId);
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			break;
		}
	}

	private boolean extract(int logId) throws SQLException, IOException {
		boolean result = false;
		// 2.2 Tiến hành extract
		try {
			// Mở file
			writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(path))));
			rawWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(rawPath))));

			SimpleDateFormat dt = new SimpleDateFormat("dd-MM-yy HH:mm");
			String currentTimeStamp = dt.format(CurrentTimeStamp.timestamp);

			Document doc = Jsoup.connect(sourceUrl).get();
			Elements provinces = doc.select("#child-item-childrens a");
			for (int i = 0; i < provinces.size(); i++) {
				int id = IdCreater.generateUniqueId();
				String dataURL = sourceUrl + provinces.get(i).attr("href");
				Document docItem = null;
				try {
					docItem = Jsoup.connect(dataURL).get();
				} catch (IOException e) {
					e.printStackTrace();
					return false;
				}
				String provinceName = provinces.get(i).attr("title");
				Element currentTemp = docItem.select(".current-temperature").first();
				String currentTemperatureText = currentTemp.text();
				String overViewText = docItem.select(".overview-caption-item.overview-caption-item-detail").text();
				String lowestTempText = docItem.select(".text-white.op-8.fw-bold:first-of-type").text().split("/")[0];
				String maximumText = docItem.selectFirst(".weather-detail .text-white.op-8.fw-bold:first-child").text()
						.split("/")[1];
				String humidityText = docItem.select(".weather-detail .text-white.op-8.fw-bold").get(1).text();
				String visionText = docItem.select(".weather-detail .text-white.op-8.fw-bold").get(2).text();
				String windText = docItem.select(".weather-detail .text-white.op-8.fw-bold").get(3).text();
				String stopPointText = docItem.select(".weather-detail .text-white.op-8.fw-bold").get(4).text();
				String uvIndexText = docItem.select(".weather-detail .text-white.op-8.fw-bold").get(5).text();
				String airQualityText = docItem.select(".air-api.air-active").text();

				rawWriter.write(id + separator + provinceName + separator + currentTimeStamp + separator
						+ currentTemperatureText + separator + overViewText + separator + lowestTempText + separator
						+ maximumText + separator + maximumText + separator + visionText + separator + windText
						+ separator + stopPointText + separator + uvIndexText + separator + airQualityText + "\n");

				// pretreatment
				int currentTemperatureNum = Integer
						.parseInt(currentTemperatureText.substring(0, currentTemperatureText.length() - 1).trim());
				int lowestTemperatureNum = Integer
						.parseInt(lowestTempText.substring(0, lowestTempText.length() - 1).trim());
				int maximumTemperatureNum = Integer.parseInt(maximumText.substring(0, maximumText.length() - 1).trim());
				Float humidityFloat = Float.parseFloat(humidityText.split("%")[0]) / 100.0f;
				Float visionNum = Float.parseFloat(visionText.split(" ")[0]);
				Float windFloat = Float.parseFloat(windText.split(" ")[0]);
				Integer stopPointNum = Integer.parseInt(stopPointText.split(" ")[0]);
				Float uvIndexFloat = Float.parseFloat(uvIndexText);

				// ghi file
				writer.write(id + separator + provinceName + separator + currentTimeStamp + separator
						+ currentTemperatureNum + separator + overViewText + separator + lowestTemperatureNum
						+ separator + maximumTemperatureNum + separator + humidityFloat + separator + visionNum
						+ separator + windFloat + separator + stopPointNum + separator + uvIndexFloat + separator
						+ airQualityText + "\n");
			}
			rawWriter.flush();
			writer.flush();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		if (ftpManager.pushFile(path, sourceConfigDao.getDistFolder(SOURCE_ID) + "/" + fileName, fileName + extension)
				&& ftpManager.pushFile(rawPath, sourceConfigDao.getDistFolder(SOURCE_ID) + "/" + rawFileName,
						rawFileName + extension)) {
			log.updateStatus(logId, "EO");
			result = true;
			System.out.println("Extract result: EO");
		} else {
			log.updateStatus(logId, "EF");
			result = false;
			System.out.println("Extract result: EF");
		}
		return result;
	}

	public static void main(String[] args) throws IOException, SQLException {
		Process1_ThoiTietEduVN firstProcessing = new Process1_ThoiTietEduVN();
		firstProcessing.execute();
	}
}
