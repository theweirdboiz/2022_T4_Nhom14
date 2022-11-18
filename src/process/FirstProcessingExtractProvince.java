package process;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import dao.IdCreater;
import dao.control.LogControllerDao;
import dao.control.SourceConfigDao;
import ftp.FTPManager;

public class FirstProcessingExtractProvince {

	private SourceConfigDao sourceConfigDao;
	private LogControllerDao log;
	private FTPManager ftpManager;
	
	private static final String SOURCE_ID = "3";
	private String source;
	private String fileName;
	private String path;
	private String destination;
	
	private PrintWriter writer;
	
	public FirstProcessingExtractProvince() throws FileNotFoundException {
		sourceConfigDao = new SourceConfigDao();
		source = sourceConfigDao.getURL(SOURCE_ID);
		
		log = new LogControllerDao();
		ftpManager = new FTPManager();
		
		Date date = new Date(Calendar.getInstance().getTime().getTime() + 1900);
		DateFormat dateFormatForFileName = new SimpleDateFormat("dd-MM-yyyy_hh-mm-ss");
		fileName = SOURCE_ID + "_" + dateFormatForFileName.format(date);
		File folderExtract = new File(sourceConfigDao.getPathFolder(SOURCE_ID));
		if (!folderExtract.exists()) folderExtract.mkdir();
		path = folderExtract.getAbsolutePath() + File.separator + fileName;
		writer = new PrintWriter(new File(path));
		destination = sourceConfigDao.getDistFolder(SOURCE_ID) + "/" + fileName;
	}
	
	public void execute()  {
		System.out.println("Extracting...");
		if (log.checkExtractedAtHourCurrent(SOURCE_ID)) {
			System.out.println("This source extracted!!");
			return;
		}
		String logId = IdCreater.createIdRandom();
		log.insertLogDefault(logId, SOURCE_ID, destination);

		try {
			Document rootHTML = Jsoup.connect(source).get();
			Element provinceTable = rootHTML.selectFirst("table");
			Elements rows = provinceTable.select("tr");
			
			for (int i = 1; i < rows.size(); i++) {
				String rowOut = IdCreater.createIdRandom() + ",";
				Elements row = rows.get(i).select("td");
				rowOut += row.get(1).text();
				writer.println(rowOut);
			}
			writer.flush();
			writer.close();
			
			if (ftpManager.pushFile(path, sourceConfigDao.getDistFolder(SOURCE_ID), fileName)) {
				log.setStatus(logId, "EO");
				System.out.println("Extract OK");
			}else {
				log.setStatus(logId, "EF");
				System.out.println("Extract Fail");
			}
		} catch (IOException e) {
			log.setStatus(logId, "EF");
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
		try {
			new FirstProcessingExtractProvince().execute();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
}
