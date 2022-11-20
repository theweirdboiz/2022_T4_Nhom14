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
import java.text.SimpleDateFormat;
import java.util.Date;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import dao.CurrentTimeStamp;
import dao.IdCreater;
import dao.Procedure;
import dao.control.SourceConfigDao;
import db.DbControlConnection;
import ftp.FTPManager;

public class FirstProcessingProvince {
	private FTPManager ftpManager;
	private Connection connection;

	private CallableStatement callStmt;
	private ResultSet rs;
	private String procedure;

	private SourceConfigDao sourceConfigDao;
	private static final int SOURCE_ID = 3;

	private String sourceUrl;
	private String fileName, rawFileName;
	private String path, rawPath;

	public FirstProcessingProvince() {
		// 1. Connect Database Control
		connection = DbControlConnection.getIntance().getConnect();
		sourceConfigDao = new SourceConfigDao();
		ftpManager = new FTPManager(SOURCE_ID);
		sourceUrl = sourceConfigDao.getURL(SOURCE_ID);
	}

	public boolean runScript() throws SQLException, IOException {
		// 1.1 Lấy một dòng dữ liệu trong log, kiểm tra source này đã được ghi vào ngày
		// hôm nay và giờ hiện tại hay chưa?
		int logId = 0;
		String status = "";
		procedure = Procedure.GET_ONE_ROW_FROM_LOG;
		callStmt = connection.prepareCall(procedure);
		callStmt.setInt(1, SOURCE_ID);
		rs = callStmt.executeQuery();
		boolean checkEmptyLog = rs.next();
		if (!checkEmptyLog) {
			// 1.1.1 Nếu chưa có dòng dữ liệu nào, ghi một log mới
			logId = IdCreater.createIdByCurrentTime();
			System.out.println("Ghi log source id: " + SOURCE_ID);
			procedure = Procedure.INSERT_RECORD;
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, logId);
			callStmt.setInt(2, SOURCE_ID);
			callStmt.execute();

			return extract(logId);
		} else {
			status = rs.getString("status");
			logId = rs.getInt("id");
		}
		// 1.1.2 Nếu đã tồn tại dữ liệu, kiểm tra trạng thái của dòng log này
		switch (status) {
		// Nếu EO -> Kết thúc luôn
		case "EO":
			System.out.println("Đã có dữ liệu vào thời điểm: " + CurrentTimeStamp.getCurrentTimeStamp());
			return true;
		// Nếu EF -> Cập nhật sang trạng thái ER -> extract
		// Nếu ER -> extract
		case "EF":
			// Cập nhật sang trạng thái ER và thời gian bắt đầu extract là thời gian hiện
			// tại
			procedure = Procedure.UPDATE_STATUS;
			callStmt = connection.prepareCall(procedure);
			callStmt.setString(1, "ER");
			callStmt.setInt(2, logId);
			callStmt.execute();

			procedure = Procedure.UPDATE_TIME_LOAD;
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, logId);
			int hour = new Date().getHours();
			callStmt.setInt(2, hour);
			callStmt.execute();
			// Kết quả extract
			return extract(logId);
		case "ER":
			procedure = Procedure.UPDATE_TIME_LOAD;
			callStmt = connection.prepareCall(procedure);
			// 91061033
			System.out.println(logId);
			callStmt.setInt(1, logId);
			hour = new Date().getHours();
			callStmt.setInt(2, hour);
			callStmt.execute();
			// Kết quả extract
			return extract(logId);
		default:
			System.out.println("Trường hợp chưa xác định!");
			break;
		}
		return false;
	}

	private boolean extract(int logId) throws SQLException {
		boolean result = false;
		// 2.Extract data theo source id ở thời điểm hiện tại (theo logId)
		System.out.println("Bắt đầu extract dữ liệu vào thời điểm: " + CurrentTimeStamp.getCurrentDate());
		String ext = ".csv";

		// 2.1 Tạo filename ở ngày và giờ hiện tại
		fileName = CurrentTimeStamp.getCurrentTimeStamp() + ext;
		rawFileName = "raw" + CurrentTimeStamp.getCurrentTimeStamp() + ext;
		// 2.1.1 Kiểm tra folder đã tồn tại hay chưa, nếu chưa thì tạo mới
		File folderExtract = new File(
				sourceConfigDao.getPathFolder(SOURCE_ID) + File.separator + CurrentTimeStamp.getCurrentDate());
		if (!folderExtract.exists()) {
			System.out.println("Tạo mới folder chứa file dữ liệu đã extract!");
			folderExtract.mkdirs();
		}
		// Lấy đường dẫn tuyệt đối của file cần ghi
		path = folderExtract.getAbsolutePath() + File.separator + fileName;
		rawPath = folderExtract.getAbsolutePath() + File.separator + rawFileName;

		// 2.2 Tiến hành extract
		PrintWriter writer = null, rawWriter = null;
		try {
			// Mở file
			writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(path))));
			rawWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(rawPath))));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Document doc = null;

		try {
			// connect source
			doc = Jsoup.connect(sourceUrl).get();
		} catch (IOException e) {
			System.out.println("Có gì đó sai sai!");
			return false;
		}
		try {
			Element provinceTable = doc.selectFirst("table");
			Elements rows = provinceTable.select("tr");
			for (int i = 1; i < rows.size(); i++) {
				String rowOut = i + ",";
				Elements row = rows.get(i).select("td");
				rowOut += row.get(1).text();
				writer.println(rowOut);
				rawWriter.println(rowOut);
				writer.flush();
				rawWriter.flush();
			}
			writer.close();
			rawWriter.close();
		} catch (NullPointerException e) {
			System.out.println("Có gì đó sai sai!");
			return false;
		}

		// 2.3 Kiểm tra kết quả extract
		// 2.3.1 Lấy thông tin FTP server từ FTPConfig
		// 2.3.2 Connect FTP server

		// 2.3.3 Extract dữ liệu thành công (đã có file trên ftp)
		// 2.3.3.1 Upload file lên FTP server
		String distFolder = sourceConfigDao.getDistFolder(SOURCE_ID);
		try {
			ftpManager.getClient().makeDirectory(distFolder);
		} catch (IOException e) {
			System.out.println("Không tạo được folder");
			e.printStackTrace();
			return false;
		}
		boolean isSuccess = false;
		try {
			isSuccess = ftpManager.pushFile(path, distFolder, fileName)
					& ftpManager.pushFile(rawPath, distFolder, rawFileName);
		} catch (IOException e) {
			System.out.println("Không push được file");
			e.printStackTrace();
		}
		// 2.3.3.1.1 Uploadfile thành công
		if (isSuccess) {
			// Cập nhật log với trạng thái 'EO'
			procedure = Procedure.UPDATE_STATUS;
			callStmt = connection.prepareCall(procedure);
			callStmt.setString(1, "EO");
			callStmt.setInt(2, logId);
			callStmt.execute();
			System.out.println("Upload file thành công vào thư mục: " + distFolder);
			result = true;
			// ftpManager.listFolder(ftpManager.getClient(), disFolder);
			// 2.3.3.1.2 Uploadfile không thành công
		} else {
			// upload file không thành công
			// Cập nhật log với trạng thái 'EF'
			procedure = Procedure.UPDATE_STATUS;
			callStmt = connection.prepareCall(procedure);
			callStmt.setString(1, "EF");
			callStmt.setInt(2, logId);
			callStmt.execute();
			System.out.println("Upload file không thành công, hãy thử lại <3");
			result = false;
		}
		// 3. Close
		// 3.1 Disconnect FTP
		// 3.2 Close Database Control
		ftpManager.close();
		connection.close();
		return result;
	}

	public static void main(String[] args) {
		FirstProcessingProvince firstProcessingProvince = new FirstProcessingProvince();
		try {
			boolean result = firstProcessingProvince.runScript();
			if (result) {
				System.out.println("Process 1: success!");
			} else {
				System.out.println("Process 1: try again!");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}