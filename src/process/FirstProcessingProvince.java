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

public class FirstProcessingProvince {
	private FTPManager ftpManager;
	private LogControllerDao log;

	private SourceConfigDao sourceConfigDao;
	private static final int SOURCE_ID = 3;

	private String sourceUrl, destinationUrl;
	private String fileName, extension, separator;
	private String path, rawPath;

	private PrintWriter writer, rawWriter;
	private Date currentDate;

	public FirstProcessingProvince() {
		sourceConfigDao = new SourceConfigDao();
		sourceUrl = sourceConfigDao.getURL(SOURCE_ID);

		log = new LogControllerDao();
		ftpManager = new FTPManager(SOURCE_ID);

		DateFormat dateFormatForFileName = new SimpleDateFormat("dd-MM-yyyy_HH");
		currentDate = new Date();
		fileName = dateFormatForFileName.format(currentDate);
		extension = ".csv";
		separator = ",";

		File folderExtract = new File(sourceConfigDao.getPathFolder(SOURCE_ID) + "/" + fileName);

		if (!folderExtract.exists()) {
			System.out.println("Create new folder: " + folderExtract.getName());
			folderExtract.mkdirs();
		}
		path = folderExtract.getAbsolutePath() + File.separator + fileName + extension;
		rawPath = folderExtract.getAbsolutePath() + File.separator + "raw_" + fileName + extension;

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
				log.updateStatus(logId, "EF");
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
				log.updateStatus(logId, "EF");
				e.printStackTrace();
			} catch (IOException e) {
				log.updateStatus(logId, "EF");

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
			writer.write(currentTimeStamp + "\n");

			Document doc = Jsoup.connect(sourceUrl).get();
			Element provinceTable = doc.selectFirst("table");
			Elements rows = provinceTable.select("tr");
			for (int i = 1; i < rows.size(); i++) {
				String rowOut = i + ",";
				Elements row = rows.get(i).select("td");
				rowOut += row.get(1).text();
				writer.println(rowOut);
				rawWriter.println(rowOut);
			}
			rawWriter.flush();
			writer.flush();

		} catch (FileNotFoundException e) {
			log.updateStatus(logId, "EF");
			e.printStackTrace();
		}

		if (ftpManager.pushFile(path, sourceConfigDao.getDistFolder(SOURCE_ID) + "/" + fileName, fileName + extension)
				&& ftpManager.pushFile(rawPath, sourceConfigDao.getDistFolder(SOURCE_ID) + "/" + fileName,
						fileName + extension)) {
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

	public static void main(String[] args) throws IOException {
		FirstProcessingProvince firstProcessingProvince = new FirstProcessingProvince();
		firstProcessingProvince.execute();
	}

}
