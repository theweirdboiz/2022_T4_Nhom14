package process;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import dao.CurrentTimeStamp;
import dao.IdCreater;
import dao.Procedure;
import dao.Query;
import dao.control.FTPConfigDao;
import dao.control.SourceConfigDao;
import db.DbControlConnection;
import db.MySQLConnection;
import ftp.FTPManager;

public class FirstProcessing implements Query, Procedure, CurrentTimeStamp {

	private FTPManager ftpManager;
	private Connection connection;

	private CallableStatement callStmt;
	private ResultSet rs;
	private String procedure;

	private SourceConfigDao sourceConfigDao;

	private static final int SOURCE_ID = 1;
	private String sourceUrl;
	private String fileName;
	private String path;

	// 1. Extract Data
	public FirstProcessing() {
		// 1.1 Connect Database Control
		connection = DbControlConnection.getIntance().getConnect();
		sourceConfigDao = new SourceConfigDao();
		ftpManager = new FTPManager();
		sourceUrl = sourceConfigDao.getURL(SOURCE_ID);
	}

	public void runScript() throws SQLException, IOException {
		// 1.2 Lấy thông tin từ SourceConfig table
		// 1.3 Lấy một source id mới từ bảng SourceConfig -> Kiểm tra source id này đã
		// tồn tại trong log và timeLoad = ngày hôm nay và có trạng thái 'EO'chưa
		procedure = Procedure.IS_EXISTED;
		callStmt = connection.prepareCall(procedure);
		callStmt.setInt(1, SOURCE_ID);
		rs = callStmt.executeQuery();
		boolean isExisted = false;
		if (rs.next()) {
			isExisted = rs.getInt(1) > 0 ? true : false;
		}
		// 1.3.1 Nếu chưa - > ghi log với status 'ER'
		// Nếu rồi -> Quay lại 1.3
		if (!isExisted) {
			procedure = Procedure.START_EXTRACT;
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, IdCreater.generateUniqueId());
			callStmt.setInt(2, SOURCE_ID);
			rs = callStmt.executeQuery();
		}
		// 2.Extract data
		System.out.println("Extracting...");
		String ext = ".csv";
		fileName = CurrentTimeStamp.getCurrentTimeStamp();

		File folderExtract = new File(sourceConfigDao.getPathFolder(SOURCE_ID));

		if (!folderExtract.exists()) {
			folderExtract.mkdir();
		}
		path = folderExtract.getAbsolutePath() + File.separator + fileName;
		// 2.2 Extract data theo source id ở 2.1
		PrintWriter writer = new PrintWriter(new File(path));

		Document doc = Jsoup.connect(sourceUrl).get();

		Elements provinces = doc.select(".megamenu a");

		writer.write(CurrentTimeStamp.getCurrentTimeStamp() + "\n");
		String separator = ", ";
		for (int i = 0; i < provinces.size(); i++) {
			int id = IdCreater.generateUniqueId();
			String dataURL = sourceUrl + provinces.get(i).attr("href");
			Document docItem = Jsoup.connect(dataURL).get();
			// province
			String provinceName = provinces.get(i).attr("title");
			// current_time
			String currentTimeText = docItem.select("#timer").text().replace("| ", "");

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

			writer.write(id + separator + provinceName + separator + currentTemperatureNum + separator + overViewText
					+ separator + lowestTemperatureNum + separator + maximumTemperatureNum + separator + humidityFloat
					+ separator + visionNum + separator + windFloat + separator + stopPointNum + separator
					+ uvIndexFloat + separator + airQualityText + "\n");
		}
		writer.flush();
		writer.close();

		// 2.3 Kiểm tra extract thành công?
		// *Thành công
		// 2.3.1 Lấy thông tin FTP server từ FTPConfig

		// 2.3.2 Connect FTP server
		// 2.3.3 Upload file với file name theo source id kiểm tra ở 2.1 -> Update
		// status = 'EO' theo source id 1.3
		boolean success = ftpManager.pushFile(path, sourceConfigDao.getDistFolder(SOURCE_ID), fileName);
		if (success) {
			procedure = Procedure.FINISH_EXTRACT;
			System.out.println("Extract success");
			// *Không thành công
		} else {
			// 2.3.4 Update status = 'EF' theo source id 2.1
			procedure = Procedure.FAIL_EXTRACT;
			System.out.println("Extract fail");

		}
		callStmt = connection.prepareCall(procedure);
		callStmt.setInt(1, 1);
		callStmt.execute();
		// 3. Close
		// 3.1 Disconnect FTP
		// 3.2 Close Database Control
		ftpManager.close();
		connection.close();
	}

	public static void main(String[] args) throws IOException, SQLException {
		FirstProcessing firstProcessing = new FirstProcessing();
		firstProcessing.runScript();
	}
}