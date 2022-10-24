package process;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import dao.control.FTPConfigDao;
import dao.control.SourceConfigDao;
import db.MySQLConnection;

public class FirstProcessing {
//	private final String WEB_URL = "https://thoitiet.vn";
	private final String DB_URL = "jdbc:mysql://localhost:3306/control";
	private final String USER_NAME = "root";
	private final String PASSWORD = "";
	MySQLConnection connectDb;

	private static final String FTP_SERVER_ADDRESS = "103.97.126.21";
	private static final int FTP_SERVER_PORT_NUMBER = 21;
	private static final int FTP_TIMEOUT = 60000;
	private static final int BUFFER_SIZE = 1024 * 1024 * 1;
	private static final String FTP_USERNAME = "ngsfihae";
	private static final String FTP_PASSWORD = "U05IIKw0HsICPNU";
	private static final String SLASH = "/";
	private FTPClient ftpClient;
	private FTPConfigDao ftpConfigDao;

	SourceConfigDao sourceConfigDao;

	public FirstProcessing() {
		connectDb = new MySQLConnection(DB_URL, USER_NAME, PASSWORD);
	}

	public void runScript() throws SQLException, IOException {
		// 1.Get 1 row from config table
		String query = "{CALL IS_EXISTED_SOURCE_ID(?)}";
		CallableStatement callStmt;
		ResultSet rs;
		callStmt = connectDb.getConnect().prepareCall(query);
		callStmt.setInt(1, 1);
		rs = callStmt.executeQuery();
		// 1.1 Check source have been run?
		boolean isExisted = false;
		while (rs.next()) {
			isExisted = rs.getInt(1) > 0 ? true : false;
		}
		if (!isExisted) {
			query = "CALL START_EXTRACT(?,?)";
			callStmt = connectDb.getConnect().prepareCall(query);
			callStmt.setInt(1, 1);
			callStmt.setInt(2, 1);
			callStmt.execute();
		}
		// 2.Extract data
		sourceConfigDao = new SourceConfigDao();
		String fileName = sourceConfigDao.getFileName();
		String url = sourceConfigDao.getUrl();

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
//			writer.write(docItem.select(".location-auto-refresh").text() + "\n");
		}
		writer.write("");
		writer.flush();
		writer.close();

		connectDb.close();
		FTPClient client = new FTPClient();

		try {
			client.connect("103.97.126.21");
			client.login("ngsfihae", "U05IIKw0HsICPNU");
		} catch (IOException e) {
			e.printStackTrace();
		}
		// upload FTP
		boolean success = uploadFTPFile(fileName);
		if (success) {
			query = "{CALL FINISH_EXTRACT(?)}";

		} else {
			query = "CALL FAIL_EXTRACT(?)";

		}
		callStmt = connectDb.getConnect().prepareCall(query);
		callStmt.setInt(1, 1);
		callStmt.execute();
	}

//

	public boolean uploadFTPFile(String ftpFilePath) {
		FileInputStream fis = null;
		boolean success = false;
		try {
			String fileName = "./data1.csv";
			fis = new FileInputStream(fileName);
			ftpClient.storeFile(fileName, fis);
			ftpClient.logout();
			success = ftpClient.completePendingCommand();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fis != null) {
					fis.close();
				}
				ftpClient.disconnect();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (success) {
			System.out.println("File " + ftpFilePath + " has been uploaded successfully.");
		}
		return success;
	}

	private void connectFTPServer() {
		ftpClient = new FTPClient();
		try {
			System.out.println("Connecting FTP server...");
			// connect to ftp server
			ftpClient.setDefaultTimeout(FTP_TIMEOUT);
			ftpClient.connect(FTP_SERVER_ADDRESS, FTP_SERVER_PORT_NUMBER);
			// run the passive mode command
			ftpClient.enterLocalPassiveMode();
			// check reply code
			if (!FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {
				disconnectFTPServer();
				throw new IOException("FTP server not respond!");
			} else {
				ftpClient.setSoTimeout(FTP_TIMEOUT);
				// login ftp server
				if (!ftpClient.login(FTP_USERNAME, FTP_PASSWORD)) {
					throw new IOException("Username or password is incorrect!");
				}
				ftpClient.setDataTimeout(FTP_TIMEOUT);
				System.out.println("Connected FTP!");
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	private void disconnectFTPServer() {
		if (ftpClient != null && ftpClient.isConnected()) {
			try {
				ftpClient.logout();
				ftpClient.disconnect();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException, SQLException {
		FirstProcessing firstProcessing = new FirstProcessing();
		firstProcessing.runScript();

	}
}