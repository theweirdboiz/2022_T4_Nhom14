package process;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.net.ftp.FTPClient;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import db.MySQLConnection;

public class FirstProcessing {
	private final String WEB_URL = "https://thoitiet.vn";
	private final String DB_URL = "jdbc:mysql://localhost:3306/control";
	private final String USER_NAME = "root";
	private final String PASSWORD = "";
	MySQLConnection connectDb;
//

	public FirstProcessing() {
		connectDb = new MySQLConnection(DB_URL, USER_NAME, PASSWORD);
	}

	public void runScript() throws SQLException, IOException {
		// 1.Get 1 row from config table
		String query = "{CALL START_EXTRACT(?,?)}";
		CallableStatement callStmt;

		callStmt = connectDb.getConnect().prepareCall(query);
		callStmt.setInt(1, 1);
		callStmt.setInt(2, 1);
		callStmt.execute();

		// 2.Extract data
		String fileName = "data1.csv";
		Document doc = Jsoup.connect(WEB_URL).get();
		PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(fileName))));
		Elements provinces = doc.select(".megamenu a");
		for (int i = 0; i < provinces.size(); i++) {
			String dataURL = WEB_URL + provinces.get(i).attr("href");
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
			writer.write(docItem.select(".air-api.air-active").text() + ",");
			// time_refresh
			writer.write(docItem.select(".location-auto-refresh").text() + "\n");
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
		uploadFTP(client);
		// download FTP
//		downloadFTP(client);

	}

	public void uploadFTP(FTPClient client) {
		FileInputStream fis = null;
		try {
			String fileName = "./data1.csv";
			fis = new FileInputStream(fileName);
			client.storeFile(fileName, fis);
			client.logout();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fis != null) {
					fis.close();
				}
				client.disconnect();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException, SQLException {
		FirstProcessing firstProcessing = new FirstProcessing();
		firstProcessing.runScript();

	}
}