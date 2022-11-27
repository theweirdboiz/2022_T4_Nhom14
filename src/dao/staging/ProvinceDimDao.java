package dao.staging;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import dao.CurrentTimeStamp;
import dao.IdCreater;
import dao.Procedure;
import dao.control.DbConfigDao;
import dao.control.LogControllerDao;
import dao.control.SourceConfigDao;
import db.DbControlConnection;
import db.DbStagingControlConnection;
import db.MySQLConnection;
import ftp.FTPManager;
import model.DbHosting;

public class ProvinceDimDao {

	private Connection connection;
	private CallableStatement callStmt;
	private ResultSet rs;
	private String procedure;
	private PreparedStatement ps;
	private String extension = ".csv";
	private String sourceUrl;
	private SourceConfigDao sourceConfigDao;
	private static final int SOURCE_ID = 3;
	private FTPManager ftpManager;

	private PrintWriter writer;
	private LogControllerDao logDao;

	private int logId;
	File ftpFolder, localFolder;
	String ftpPath, localPath, fileName;

	public ProvinceDimDao() {
		connection = DbStagingControlConnection.getIntance().getConnect();
		sourceConfigDao = new SourceConfigDao();
		sourceUrl = sourceConfigDao.getURL(SOURCE_ID);
		ftpManager = new FTPManager();
		logDao = new LogControllerDao();
		logId = IdCreater.createIdByCurrentTime();

		fileName = CurrentTimeStamp.getCurrentTimeStamp();
		ftpFolder = new File(sourceConfigDao.getFtpFolder(SOURCE_ID));
		localFolder = new File(sourceConfigDao.getLocalFolder(SOURCE_ID));

		if (!localFolder.exists()) {
			System.out.println("Create new folder: " + localFolder.getPath());
			localFolder.mkdir();
		}
		ftpPath = ftpFolder.getPath() + "/" + fileName + extension;

		localPath = localFolder.getAbsolutePath() + File.separator + fileName + extension;
	}

	public boolean execute() throws IOException {
		System.out.println(">> Start: load provincedim into staging");
		int logId = IdCreater.createIdByCurrentTime();
		logDao.insertRecord(logId, SOURCE_ID, localPath, ftpPath);
		return extract(logId);
	}

	public boolean load() throws SQLException, IOException {
		connection = DbControlConnection.getIntance().getConnect();
		boolean result = false;
		procedure = Procedure.GET_ONE_DIM;
		callStmt = connection.prepareCall(procedure);
		callStmt.setInt(1, SOURCE_ID);
		rs = callStmt.executeQuery();
		if (rs.next()) {
			int logId = rs.getInt("id");
			String path = rs.getString("ftpPath");
			try {
				connection = DbStagingControlConnection.getIntance().getConnect();
				BufferedReader br = new BufferedReader(new FileReader(new File(path)));
				String line;
				while ((line = br.readLine()) != null) {
					String[] elms = line.split(",");
					procedure = Procedure.LOAD_PROVINCE_DIM;
					callStmt = connection.prepareCall(procedure);
					callStmt.setInt(1, Integer.parseInt(elms[0]));
					callStmt.setString(2, elms[1]);
					callStmt.setString(3, elms[2]);
					callStmt.execute();
				}
				logDao.updateStatus(logId, "EL");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	private boolean extract(int logId) throws IOException {
		// extract
		boolean result = false;
		Document doc;
		try {
			doc = Jsoup.connect(sourceUrl).get();
			// TODO Auto-generated catch block
			Element provinceTable = doc.selectFirst("table");
			Elements rows = provinceTable.select("tr");

			writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(localPath))));
			for (int i = 1; i < rows.size(); i++) {
				String rowOut = i + ",";
				Elements row = rows.get(i).select("td");
				rowOut += row.get(1).text() + "," + CurrentTimeStamp.getCurrentTimeStamp();
				writer.println(rowOut);
			}
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (ftpManager.pushFile(localPath, ftpFolder.getPath(), fileName + extension)) {
			logDao.updateStatus(logId, "EO");
			result = true;
			System.out.println(">> End: extract result: EO");
		} else {
			result = false;
			logDao.updateStatus(logId, "EF");
			System.out.println(">> End: extract result: EF");
		}
		return result;
	}

	public static void main(String[] args) throws SQLException, IOException {
		ProvinceDimDao provinceDimDao = new ProvinceDimDao();
		provinceDimDao.execute();
		provinceDimDao.load();
	}
}
