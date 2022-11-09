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

import org.apache.commons.net.ftp.FTPClient;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import dao.CurrentTimeStamp;
import dao.IdCreater;
import dao.Procedure;
import dao.Query;

import dao.control.SourceConfigDao;
import db.DbControlConnection;

import ftp.FTPManager;

public class FirstProcessingThoiTietVn implements Query, Procedure, CurrentTimeStamp {

	private FTPManager ftpManager;
	private Connection connection;

	private CallableStatement callStmt;
	private ResultSet rs;
	private String procedure;

	private SourceConfigDao sourceConfigDao;

	private static final int SOURCE_ID = 1;
	private String sourceUrl;
	private String fileName, rawFileName;
	private String path, rawPath;

	// 1. Extract Data
	public FirstProcessingThoiTietVn() {
		// 1.1 Connect Database Control
		connection = DbControlConnection.getIntance().getConnect();
		sourceConfigDao = new SourceConfigDao();
		ftpManager = new FTPManager();
		sourceUrl = sourceConfigDao.getURL(SOURCE_ID);
	}

	public boolean runScript() throws SQLException, IOException {
		// 1. Trước khi chạy script, vào ghi log, source nào? thời gian? trạng thái?
		// 1.1 Kiểm tra source này đã được ghi vào ngày hôm nay và giờ hiện tại hay
		// chưa?
		procedure = Procedure.IS_EXISTED;
		callStmt = connection.prepareCall(procedure);
		callStmt.setInt(1, SOURCE_ID);
		rs = callStmt.executeQuery();
		boolean isExisted = false;
		int logId = IdCreater.createIdByCurrentTime();
		if (rs.next()) {
			isExisted = rs.getInt(1) > 0 ? true : false;
		}
		// 1.1.2 Nếu chưa - > ghi log với status 'ER', ngược lại -> kết thúc
		if (!isExisted) {
			System.out.println("Ghi log");
			procedure = Procedure.START_EXTRACT;
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, logId);
			callStmt.setInt(2, SOURCE_ID);
			callStmt.execute();
		} else {
			System.out.println("Đã có dữ liệu vào thời điểm: " + CurrentTimeStamp.getCurrentTimeStamp());
			return false;
		}
		// 2.Extract data theo source id đã xác định ở bước 1
		System.out.println("Bắt đầu extract dữ liệu vào thời điểm: " + CurrentTimeStamp.getCurrentDate());
		String ext = ".csv";

		// Tạo filename ở ngày và giờ hiện tại
		fileName = CurrentTimeStamp.getCurrentTimeStamp() + ext;
		rawFileName = "raw" + CurrentTimeStamp.getCurrentTimeStamp() + ext;
//		System.out.println(fileName + "\t" + rawFileName);
		// Kiểm tra folder đã tồn tại hay chưa, nếu chưa thì tạo mới
		File folderExtract = new File(
				sourceConfigDao.getPathFolder(SOURCE_ID) + File.separator + CurrentTimeStamp.getCurrentDate());
		if (!folderExtract.exists()) {
			System.out.println("Tạo mới folder chưa file dữ liệu đã extract!");
			folderExtract.mkdirs();
		}

		// Lấy đường dẫn tuyệt đối của file cần ghi
		path = folderExtract.getAbsolutePath() + File.separator + fileName;
		rawPath = folderExtract.getAbsolutePath() + File.separator + rawFileName;

		// 2.1 Tiến hành extract

		PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(path))));
		PrintWriter rawWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(rawPath))));

		Document doc = Jsoup.connect(sourceUrl).get();

		Elements provinces = doc.select(".megamenu a");

		String separator = ", ";
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

			rawWriter.write(id + separator + provinceName + separator + currentTemperatureText + separator
					+ overViewText + separator + lowestTempText + separator + maximumText + separator + maximumText
					+ separator + visionText + separator + windText + separator + stopPointText + separator
					+ uvIndexText + separator + airQualityText + "\n");

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
		rawWriter.flush();
		writer.flush();
		rawWriter.close();
		writer.close();

		// 2.2 Kiểm tra kết quả extract
		// 2.2.1 Lấy thông tin FTP server từ FTPConfig
		// 2.2.2 Connect FTP server

		// 2.3 Thành công
		// 2.3.1 Upload file lên FTP server
		String disFolder = sourceConfigDao.getDistFolder(SOURCE_ID);
		ftpManager.getClient().makeDirectory(disFolder);

		boolean success = ftpManager.pushFile(path, disFolder, fileName)
				& ftpManager.pushFile(rawPath, disFolder, rawFileName);
		if (success) {
			// 2.3.2 Cập nhật log với trạng thái 'EO'
			procedure = Procedure.FINISH_EXTRACT;
			System.out.println("Extract dữ liệu thành công vào folder trên FTP: " + disFolder);
			ftpManager.listFolder(ftpManager.getClient(), disFolder);
			// 2.4 Không thành công
		} else {
			// Cập nhật log với trạng thái 'EF'
			procedure = Procedure.FAIL_EXTRACT;
			System.out.println("Extract dữ liệu không thành công, vui lòng kiểm tra lại quá trình upload file");
		}
		callStmt = connection.prepareCall(procedure);
		callStmt.setInt(1, logId);
		callStmt.execute();

		// 3. Close
		// 3.1 Disconnect FTP
		// 3.2 Close Database Control
		ftpManager.close();
		connection.close();
		return true;
	}

	public static void main(String[] args) throws IOException, SQLException {
		FirstProcessingThoiTietVn firstProcessing = new FirstProcessingThoiTietVn();
		boolean result = firstProcessing.runScript();
		if (result) {
			System.out.println("Process 1: success!");
		} else {
			System.out.println("Process 1: try again!");
		}
	}
}