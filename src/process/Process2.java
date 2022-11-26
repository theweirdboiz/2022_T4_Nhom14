package process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;

import dao.IdCreater;
import dao.Procedure;
import dao.control.LogControllerDao;
import dao.control.SourceConfigDao;
import dao.staging.DateDimDao;
import dao.staging.ProvinceDimDao;
import dao.staging.RawWeatherFactDao;
import dao.staging.TimeDimdao;
import dao.staging.WeatherFactDao;
import ftp.FTPManager;

public class Process2 implements Procedure {
	private static final int SOURCE_PROVINCE_ID = 3;
	private static final int SOURCE_DATE_DIM_ID = 4;
	private static final int SOURCE_TIME_DIM_ID = 5;

	private ResultSet rs;

	private FTPManager ftpManager;

	private LogControllerDao logDao;
	private SourceConfigDao sourceConfigDao;
	private DateDimDao dateDimDao;
	private TimeDimdao timeDimdao;
	private ProvinceDimDao provinceDimDao;
	private WeatherFactDao weatherFactDao;
	private String procedure;
	private int logId;

	public Process2() {
		logDao = new LogControllerDao();
		sourceConfigDao = new SourceConfigDao();
		logId = logDao.getLogId();
		dateDimDao = new DateDimDao();
		timeDimdao = new TimeDimdao();
		provinceDimDao = new ProvinceDimDao();
		weatherFactDao = new WeatherFactDao();
		ftpManager = new FTPManager(SOURCE_PROVINCE_ID);
	}

	private boolean loadProvinceDimIntoStaging(int logId, String destination) {
		boolean result = false;
		BufferedReader br;
		try {
			br = ftpManager.getReaderFileInFTPServer(destination);
			String line;
			System.out.println(">> Start: load province dim into staging");
			while ((line = br.readLine()) != null) {
				String[] elms = line.split(",");
				int id = Integer.parseInt(elms[0].trim());
				String name = elms[1].trim();
				result = provinceDimDao.loadByLine(id, name);
			}
			logDao.updateStatus(logId, "EL");
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(">> End: " + result);
		return result;
	}

	private boolean loadDateDimIntoStaging() {
		boolean result = true;
		File file = new File(sourceConfigDao.getURL(SOURCE_DATE_DIM_ID));
		System.out.println(file.getAbsolutePath());
		if (!file.exists()) {
			System.out.println("Create date_dim.csv");
			dateDimDao.createFile();
			System.out.println("--------------------------");
		}
		if (!dateDimDao.isDateDimExisted()) {
			BufferedReader br;
			try {
				br = new BufferedReader(new FileReader(file));
				String line;
				System.out.println(">> Start: load date dim into staging");
				while ((line = br.readLine()) != null) {
					String[] elms = line.split(",");
					int id = Integer.parseInt(elms[0].trim());
					String date = elms[1].trim();
					int year = Integer.parseInt(elms[2].trim());
					int month = Integer.parseInt(elms[3].trim());
					int day = Integer.parseInt(elms[4].trim());
					String dayOfWeek = elms[5].trim();
					result = dateDimDao.loadByLine(id, date, year, month, day, dayOfWeek);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println(">> End: " + result);
		}
		return result;
	}

	private boolean loadTimeDimIntoStaging() {
		boolean result = true;
		File file = new File(sourceConfigDao.getURL(SOURCE_TIME_DIM_ID));
		System.out.println(file.getAbsolutePath());
		if (!file.exists()) {
			System.out.println("Create time_dim.csv");
			timeDimdao.createFile();
			System.out.println("--------------------------");
		}
		if (!timeDimdao.isTimeDimExisted()) {
			BufferedReader br;
			try {
				br = new BufferedReader(new FileReader(file));
				String line;
				System.out.println(">> Start: load time dim into staging");
				while ((line = br.readLine()) != null) {
					String[] elms = line.split(",");
					int id = Integer.parseInt(elms[0].trim());
					String time = elms[1].trim();
					result = timeDimdao.loadByLine(id, time);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println(">> End: " + result);
		}
		return result;
	}

	private boolean loadWeatherFactIntoStaging() {
		return weatherFactDao.transformFact();
	}

	public boolean excute() {
		boolean result = false;
		int logId = logDao.getLogId();
		int sourceId = logDao.getSourceIdByLogId(logId);
		String destination = logDao.getDestinationByLogId(logId);
		String status = logDao.getStatusByLogId(logId);
		if (!status.equals("EO")) {
			System.out.println("No any new file in FTP server");
			return false;
		}
		System.out.println(destination + " ready to loading");
		loadProvinceDimIntoStaging(logId, destination);
		loadDateDimIntoStaging();
		loadTimeDimIntoStaging();
		loadWeatherFactIntoStaging();
		return result;
	}

	public static void main(String[] args) throws SQLException, IOException {
		Process2 sp = new Process2();
		sp.excute();
	}

}
