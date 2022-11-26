package process;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

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
import dao.control.LogControllerDao;
import dao.control.SourceConfigDao;
import ftp.FTPManager;

public class Process1_ProvinceVN {
	private FTPManager ftpManager;
	private LogControllerDao log;

	private SourceConfigDao sourceConfigDao;
	private static final int SOURCE_ID = 3;

	private String sourceUrl, ftpPath, localPath, rawFtpPath, rawLocalPath;
	private String fileName, rawFileName, extension, separator;

	private PrintWriter writer, rawWriter;
	private Date currentDate;
	private File localFolder;

	public Process1_ProvinceVN() {
		sourceConfigDao = new SourceConfigDao();
		sourceUrl = sourceConfigDao.getURL(SOURCE_ID);

		log = new LogControllerDao();
		ftpManager = new FTPManager(SOURCE_ID);

		DateFormat dateFormatForFileName = new SimpleDateFormat("dd-MM-yyyy_HH");
		currentDate = new Date();
		fileName = dateFormatForFileName.format(currentDate);
		rawFileName = "raw_" + fileName;
		extension = ".csv";
		separator = ",";

		localFolder = new File(sourceConfigDao.getPathFolder(SOURCE_ID) + "/" + fileName);

		if (!localFolder.exists()) {
			System.out.println("Create new folder: " + localFolder.getName());
			localFolder.mkdirs();
		}
		ftpPath = sourceConfigDao.getPathFolder(SOURCE_ID) + "/" + fileName;
		rawFtpPath = sourceConfigDao.getPathFolder(SOURCE_ID) + "/" + fileName;
		localPath = localFolder.getPath() + File.separator + fileName + extension;
		rawLocalPath = localFolder.getPath() + File.separator + "raw_" + fileName + extension;
	}

	public void execute() throws IOException {
		DateFormat dateFormate = new SimpleDateFormat("yy-MM-dd HH:MM-ss");
		System.out.println(">> Start: extract source_id: " + SOURCE_ID + "\tat time: " + fileName);
		String preStatus = null;
		String timeLoad = null;
		try {
			preStatus = log.getOneRowInformation(SOURCE_ID).getString("status");
			timeLoad = log.getOneRowInformation(SOURCE_ID).getString("timeLoad");
		} catch (SQLException e1) {
			preStatus = "";
			timeLoad = "";
		}
		currentDate = new Date();
		if (timeLoad.equals(dateFormate.format(currentDate)) && preStatus.equals("EO")) {
			System.out.println("This source id:" + SOURCE_ID + " has been loaded!");
			return;
		}
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
			Element provinceTable = doc.selectFirst("table");
			Elements rows = provinceTable.select("tr");
			for (int i = 1; i < rows.size(); i++) {
				String rowOut = i + ",";
				Elements row = rows.get(i).select("td");
				rowOut += row.get(1).text() + "," + currentTimeStamp;
				writer.println(rowOut);
				rawWriter.println(rowOut);
			}
			rawWriter.flush();
			writer.flush();

		} catch (FileNotFoundException e) {
			log.updateStatus(logId, "EF");
			e.printStackTrace();
		}

		if (ftpManager.pushFile(localPath, ftpPath, fileName + extension)
				&& ftpManager.pushFile(rawLocalPath, rawFtpPath, rawFileName + extension)) {
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

	public static void main(String[] args) throws IOException {
		Process1_ProvinceVN firstProcessingProvince = new Process1_ProvinceVN();
		firstProcessingProvince.execute();
	}

}
