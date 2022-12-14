package process;

import java.io.File;
import java.io.FileInputStream;
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

import org.apache.commons.net.ftp.FTPClient;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import dao.CurrentTimeStamp;
import dao.IdCreater;
import dao.Procedure;
import dao.Query;
import dao.control.LogControllerDao;
import dao.control.SourceConfigDao;
import db.DbControlConnection;

import ftp.FTPManager;

public class FirstProcessingThoiTietVn implements Query, Procedure, CurrentTimeStamp {

	private SourceConfigDao sourceConfigDao;
	private LogControllerDao log;
	private FTPManager ftpManager;

	private static final String SOURCE_ID = "1";
	private String source;
	private String fileName;
	private String path;
	private String destination;
	
	private Date current;
	
	private PrintWriter writer;

	public FirstProcessingThoiTietVn() throws FileNotFoundException {
		sourceConfigDao = new SourceConfigDao();
		source = sourceConfigDao.getURL(SOURCE_ID);

		log = new LogControllerDao();
		ftpManager = new FTPManager();
		
		current = new Date(Calendar.getInstance().getTime().getTime() + 1900);
		DateFormat dateFormatForFileName = new SimpleDateFormat("dd-mm-yyyy_hh-mm-ss");
		fileName = SOURCE_ID + "_" + dateFormatForFileName.format(current);
		File folderExtract = new File(sourceConfigDao.getPathFolder(SOURCE_ID));
		if (!folderExtract.exists()) folderExtract.mkdir();
		path = folderExtract.getAbsolutePath() + File.separator + fileName;
		writer = new PrintWriter(new File(path));
		destination = sourceConfigDao.getDistFolder(SOURCE_ID) + "/" + fileName;

	}

	public void execute() {
		System.out.println("Extracting...");
		if (log.checkExtractedAtHourCurrent(SOURCE_ID)) {
			System.out.println("This source extracted!!");
			return;
		}
		
		String logId = IdCreater.createIdRandom();
		log.insertLogDefault(logId, SOURCE_ID, destination);
		try {
			Document root = Jsoup.connect(source).get();

			DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
			writer.write(dateFormat.format(current) + "\n");

			Elements provinces = root.select(".megamenu a");

			String separator = ", ";
			for (int i = 0; i < provinces.size(); i++) {
				String id = IdCreater.createIdRandom();
				String dataURL = source + provinces.get(i).attr("href");
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

				writer.println(id + separator + provinceName + separator + currentTemperatureNum + separator
						+ lowestTemperatureNum  + separator + maximumTemperatureNum  + separator + humidityFloat 
						+ separator + overViewText + separator + windFloat  + separator + visionNum + separator
						+ stopPointNum + separator + uvIndexFloat + separator + airQualityText);

			}
			writer.flush();
			writer.close();

			if (ftpManager.pushFile(path, sourceConfigDao.getDistFolder(SOURCE_ID), fileName)) {
				log.setStatus(logId, "EO");
				System.out.println("Extract OK");
			} else {
				log.setStatus(logId, "EF");
				System.out.println("Extract Fail");
			}
		} catch (IOException e) {
			log.setStatus(logId, "EF");
			e.printStackTrace();
		}
	}

//	private FTPManager ftpManager;
//	private Connection connection;
//
//	private CallableStatement callStmt;
//	private ResultSet rs;
//	private String procedure;
//
//	private SourceConfigDao sourceConfigDao;
//
//	private static final int SOURCE_ID = 1;
//	private String sourceUrl;
//	private String fileName, rawFileName;
//	private String path, rawPath;
//
//	// 1. Extract Data
//	public FirstProcessingThoiTietVn() {
//		// 1.1 Connect Database Control
//		connection = DbControlConnection.getIntance().getConnect();
//		sourceConfigDao = new SourceConfigDao();
//		ftpManager = new FTPManager();
//		sourceUrl = sourceConfigDao.getURL(SOURCE_ID);
//	}
//
//	public boolean runScript() throws SQLException, IOException {
//		// 1. Tr?????c khi ch???y script, v??o ghi log, source n??o? th???i gian? tr???ng th??i?
//		// 1.1 Ki???m tra source n??y ???? ???????c ghi v??o ng??y h??m nay v?? gi??? hi???n t???i hay
//		// ch??a?
//		procedure = Procedure.IS_EXISTED;
//		callStmt = connection.prepareCall(procedure);
//		callStmt.setInt(1, SOURCE_ID);
//		rs = callStmt.executeQuery();
//		boolean isExisted = false;
//		String logId = IdCreater.createIdByCurrentTime();
//		if (rs.next()) {
//			isExisted = rs.getInt(1) > 0 ? true : false;
//		}
//		// 1.1.2 N???u ch??a - > ghi log v???i status 'ER', ng?????c l???i -> k???t th??c
//		if (!isExisted) {
//			System.out.println("Ghi log");
//			procedure = Procedure.START_EXTRACT;
//			callStmt = connection.prepareCall(procedure);
//			callStmt.setInt(1, logId);
//			callStmt.setInt(2, SOURCE_ID);
//			callStmt.execute();
//		} else {
//			System.out.println("???? c?? d??? li???u v??o th???i ??i???m: " + CurrentTimeStamp.getCurrentTimeStamp());
//			return false;
//		}
//		// 2.Extract data theo source id ???? x??c ?????nh ??? b?????c 1
//		System.out.println("B???t ?????u extract d??? li???u v??o th???i ??i???m: " + CurrentTimeStamp.getCurrentDate());
//		String ext = ".csv";
//
//		// T???o filename ??? ng??y v?? gi??? hi???n t???i
//		fileName = CurrentTimeStamp.getCurrentTimeStamp() + ext;
//		rawFileName = "raw" + CurrentTimeStamp.getCurrentTimeStamp() + ext;
////		System.out.println(fileName + "\t" + rawFileName);
//		// Ki???m tra folder ???? t???n t???i hay ch??a, n???u ch??a th?? t???o m???i
//		File folderExtract = new File(
//				sourceConfigDao.getPathFolder(SOURCE_ID) + File.separator + CurrentTimeStamp.getCurrentDate());
//		if (!folderExtract.exists()) {
//			System.out.println("T???o m???i folder ch??a file d??? li???u ???? extract!");
//			folderExtract.mkdirs();
//		}
//
//		// L???y ???????ng d???n tuy???t ?????i c???a file c???n ghi
//		path = folderExtract.getAbsolutePath() + File.separator + fileName;
//		rawPath = folderExtract.getAbsolutePath() + File.separator + rawFileName;
//
//		// 2.1 Ti???n h??nh extract
//
//		PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(path))));
//		PrintWriter rawWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(rawPath))));
//
//		Document doc = Jsoup.connect(sourceUrl).get();
//
//		Elements provinces = doc.select(".megamenu a");
//
//		String separator = ", ";
//		for (int i = 0; i < provinces.size(); i++) {
//			int id = IdCreater.generateUniqueId();
//			String dataURL = sourceUrl + provinces.get(i).attr("href");
//			Document docItem = Jsoup.connect(dataURL).get();
//			// province
//			String provinceName = provinces.get(i).attr("title");
//
//			Element currentTemp = docItem.select(".current-temperature").first();
//			// current_temperature
//			String currentTemperatureText = currentTemp.text();
//			// overview
//			String overViewText = docItem.select(".overview-caption-item.overview-caption-item-detail").text();
//			// lowest_temp
//			String lowestTempText = docItem.select(".text-white.op-8.fw-bold:first-of-type").text().split("/")[0];
//			// maximum_temp
//			String maximumText = docItem.selectFirst(".weather-detail .text-white.op-8.fw-bold:first-child").text()
//					.split("/")[1];
//			// humidity
//			String humidityText = docItem.select(".weather-detail .text-white.op-8.fw-bold").get(1).text();
//			// vision
//			String visionText = docItem.select(".weather-detail .text-white.op-8.fw-bold").get(2).text();
//			// wind
//			String windText = docItem.select(".weather-detail .text-white.op-8.fw-bold").get(3).text();
//			// stop_point
//			String stopPointText = docItem.select(".weather-detail .text-white.op-8.fw-bold").get(4).text();
//			// uv_index
//			String uvIndexText = docItem.select(".weather-detail .text-white.op-8.fw-bold").get(5).text();
//			// air_quality
//			String airQualityText = docItem.select(".air-api.air-active").text();
//
//			rawWriter.write(id + separator + provinceName + separator + currentTemperatureText + separator
//					+ overViewText + separator + lowestTempText + separator + maximumText + separator + maximumText
//					+ separator + visionText + separator + windText + separator + stopPointText + separator
//					+ uvIndexText + separator + airQualityText + "\n");
//
//			// pretreatment
//			int currentTemperatureNum = Integer
//					.parseInt(currentTemperatureText.substring(0, currentTemperatureText.length() - 1).trim());
//			int lowestTemperatureNum = Integer
//					.parseInt(lowestTempText.substring(0, lowestTempText.length() - 1).trim());
//			int maximumTemperatureNum = Integer.parseInt(maximumText.substring(0, maximumText.length() - 1).trim());
//			float humidityFloat = Float.parseFloat(humidityText.split("%")[0]) / 100.0f;
//			float visionNum = Float.parseFloat(visionText.split(" ")[0]);
//			float windFloat = Float.parseFloat(windText.split(" ")[0]);
//			int stopPointNum = Integer.parseInt(stopPointText.split(" ")[0]);
//			Float uvIndexFloat = Float.parseFloat(uvIndexText);
//
//			writer.write(id + separator + provinceName + separator + currentTemperatureNum + separator + overViewText
//					+ separator + lowestTemperatureNum + separator + maximumTemperatureNum + separator + humidityFloat
//					+ separator + visionNum + separator + windFloat + separator + stopPointNum + separator
//					+ uvIndexFloat + separator + airQualityText + "\n");
//
//		}
//		rawWriter.flush();
//		writer.flush();
//		rawWriter.close();
//		writer.close();
//
//		// 2.2 Ki???m tra k???t qu??? extract
//		// 2.2.1 L???y th??ng tin FTP server t??? FTPConfig
//		// 2.2.2 Connect FTP server
//
//		// 2.3 Th??nh c??ng
//		// 2.3.1 Upload file l??n FTP server
//		String disFolder = sourceConfigDao.getDistFolder(SOURCE_ID);
//		ftpManager.getClient().makeDirectory(disFolder);
//
//		boolean success = ftpManager.pushFile(path, disFolder, fileName)
//				& ftpManager.pushFile(rawPath, disFolder, rawFileName);
//		if (success) {
//			// 2.3.2 C???p nh???t log v???i tr???ng th??i 'EO'
//			procedure = Procedure.FINISH_EXTRACT;
//			System.out.println("Extract d??? li???u th??nh c??ng v??o folder tr??n FTP: " + disFolder);
//			ftpManager.listFolder(ftpManager.getClient(), disFolder);
//			// 2.4 Kh??ng th??nh c??ng
//		} else {
//			// C???p nh???t log v???i tr???ng th??i 'EF'
//			procedure = Procedure.FAIL_EXTRACT;
//			System.out.println("Extract d??? li???u kh??ng th??nh c??ng, vui l??ng ki???m tra l???i qu?? tr??nh upload file");
//		}
//		callStmt = connection.prepareCall(procedure);
//		callStmt.setInt(1, logId);
//		callStmt.execute();
//
//		// 3. Close
//		// 3.1 Disconnect FTP
//		// 3.2 Close Database Control
//		ftpManager.close();
//		connection.close();
//		return true;
//	}

	public static void main(String[] args) throws IOException, SQLException {
		FirstProcessingThoiTietVn firstProcessingThoiTietVn = new FirstProcessingThoiTietVn();
		firstProcessingThoiTietVn.execute();
	}
}