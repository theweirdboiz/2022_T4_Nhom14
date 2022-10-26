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
import org.apache.commons.net.ftp.FTPReply;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import dao.Procedure;
import dao.Query;
import dao.control.FTPConfigDao;
import dao.control.SourceConfigDao;
import db.DbControlConnection;
import db.MySQLConnection;
import ftp.FTPManager;

public class FirstProcessing implements Query, Procedure {
	private SourceConfigDao sourceConfigDao;
	private CallableStatement callStmt;
	private ResultSet rs;
	private String procedure;
	private FTPManager ftpManager;
	Connection connection;

	// 1. Extract Data
	public FirstProcessing() {
		// 1.1 Connect Database Control
		sourceConfigDao = new SourceConfigDao();
		connection = DbControlConnection.getIntance().getConnect();
		ftpManager = new FTPManager();
	}

	public void runScript() throws SQLException, IOException {
		// 1.2 Lấy thông tin từ SourceConfig table
		// 1.3 Lấy một source id mới từ bảng SourceConfig -> Kiểm tra source id này đã
		// tồn tại trong log và timeLoad = ngày hôm nay và có trạng thái 'EO'chưa
		int sourceId = sourceConfigDao.getId();
		procedure = Procedure.IS_EXISTED;
		callStmt = connection.prepareCall(procedure);
		callStmt.setInt(1, sourceId);
		rs = callStmt.executeQuery();
		boolean isExisted = false;
		while (rs.next()) {
			isExisted = rs.getInt(1) > 0 ? true : false;
		}
		// 1.3.1 Nếu chưa - > ghi log với status 'ER'
		if (!isExisted) {
			procedure = Procedure.START_EXTRACT;
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, 1);
			callStmt.setInt(2, sourceId);
			rs = callStmt.executeQuery();
		}
		// 1.3.2 Nếu rồi -> Quay lại 1.3

		// 2.Extract data

		// 2.1 Lấy source id nào có trạng thái 'ER' và ngày ghi log = ngày hôm nay
		// (tránh trường hợp những ngày trước chưa được extract)
		procedure = Procedure.GET_CURRENT_SOURCE_ID;
		callStmt = connection.prepareCall(procedure);
		rs = callStmt.executeQuery();
		rs.next();
		int sourceIdCurrent = rs.getInt("sourceId");
		String fileName = sourceConfigDao.getFileName(sourceIdCurrent);
		String url = sourceConfigDao.getUrl(sourceIdCurrent);

		// 2.2 Extract data theo source id ở 2.1
		Document doc = Jsoup.connect(url).get();
		PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(fileName))));
		Elements provinces = doc.select(".megamenu a");
		for (int i = 0; i < provinces.size(); i++) {
			String dataURL = url + provinces.get(i).attr("href");
			Document docItem = Jsoup.connect(dataURL).get();
			// province
			writer.write(provinces.get(i).attr("title") + ",");
			// current_time
			writer.write(docItem.select("#timer").text().replace("| ", "") + ",");
			Element currentTemp = docItem.select(".current-temperature").first();
			// current_temperature
			writer.write(currentTemp.text() + ",");
			// overview
			writer.write(docItem.select(".overview-caption-item.overview-caption-item-detail").text() + ","); // lowest_temp
			// lowest
			writer.write(docItem.select(".text-white.op-8.fw-bold:first-of-type").text().split("/")[0] + ",");
			// maximum_temp
			writer.write(
					docItem.selectFirst(".weather-detail .text-white.op-8.fw-bold:first-child").text().split("/")[1]
							+ ",");
			// humidity
			writer.write(docItem.select(".weather-detail .text-white.op-8.fw-bold").get(1).text() + ",");
			// vision
			writer.write(docItem.select(".weather-detail .text-white.op-8.fw-bold").get(2).text() + ",");
			// wind
			writer.write(docItem.select(".weather-detail .text-white.op-8.fw-bold").get(3).text() + ",");
			// stop_point
			writer.write(docItem.select(".weather-detail .text-white.op-8.fw-bold").get(4).text() + ",");
			// uv_index
			writer.write(docItem.select(".weather-detail .text-white.op-8.fw-bold").get(5).text() + ",");
			// air_quality
			writer.write(docItem.select(".air-api.air-active").text() + "\n");
			// time_refresh
		}
		writer.write("");
		writer.flush();
		writer.close();

		// 2.3 Kiểm tra extract thành công?
		// *Thành công
		// 2.3.1 Lấy thông tin FTP server từ FTPConfig

		// 2.3.2 Connect FTP server
		// 2.3.3 Upload file với file name theo source id kiểm tra ở 2.1 -> Update
		// status = 'EO' theo source id 1.3
		boolean success = ftpManager.pushFile(fileName, fileName);
//				uploadFTPFile(fileName);
		if (success) {
			procedure = Procedure.FINISH_EXTRACT;
			// *Không thành công
		} else {
			// 2.3.4 Update status = 'EF' theo source id 2.1
			procedure = Procedure.FAIL_EXTRACT;

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