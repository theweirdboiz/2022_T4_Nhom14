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
import dao.staging.TimeDimdao;
import dao.staging.WeatherFactDao;
import db.DbControlConnection;
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
//	private int logId;

	public Process2() {
		logDao = new LogControllerDao();
		sourceConfigDao = new SourceConfigDao();
//		logId = logDao.getLogId();
		dateDimDao = new DateDimDao();
		timeDimdao = new TimeDimdao();
		provinceDimDao = new ProvinceDimDao();
		weatherFactDao = new WeatherFactDao();
		ftpManager = new FTPManager(SOURCE_PROVINCE_ID);
	}

	private boolean loadProvinceDimIntoStaging() throws IOException, SQLException {
		provinceDimDao.execute();
		return provinceDimDao.load();
	}

	private boolean loadDateDimIntoStaging() throws SQLException, IOException {
		dateDimDao.execute();
		return dateDimDao.load();
	}

	private boolean loadTimeDimIntoStaging() throws IOException, SQLException {
		timeDimdao.execute();
		return timeDimdao.load();
	}

	private boolean loadWeatherFactIntoStaging(String ftpPath) throws IOException {
		boolean result = false;
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(ftpPath)));
			String line;
			System.out.println(">> Start: loading raw fact");
			while ((line = br.readLine()) != null) {
				String[] elms = line.split(",");
				weatherFactDao.loadRawFactByLine(Integer.parseInt(elms[0].trim()), elms[1].trim(), elms[2].trim(),
						Integer.parseInt(elms[3].trim()), elms[4].trim(), Integer.parseInt(elms[5].trim()),
						Integer.parseInt(elms[6].trim()), Float.parseFloat(elms[7].trim()),
						Float.parseFloat(elms[8].trim()), Float.parseFloat(elms[9].trim()),
						Integer.parseInt(elms[10].trim()), Float.parseFloat(elms[11].trim()), elms[12].trim());
			}
			result = true;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		}
		System.out.println(">> End: " + result);
		return result;
	}

	private boolean transFormWeatherFactIntoStaging() {
		return weatherFactDao.transformFact();
	}

	public void excute() throws SQLException, IOException {
		loadDateDimIntoStaging();
		loadTimeDimIntoStaging();
		loadProvinceDimIntoStaging();
		rs = logDao.getOneFact();
		if (rs.next()) {
			int logId = rs.getInt("id");
			String status = rs.getString("status");
			String ftpPath = rs.getString("ftpPath");
			if (status.equals("EO")) {
				loadWeatherFactIntoStaging(ftpPath);
				logDao.updateStatus(logId, "EL");
				transFormWeatherFactIntoStaging();
			}
			return;
		}
		System.out.println("Not any new file!");

	}

	public static void main(String[] args) throws SQLException, IOException {
		Process2 sp = new Process2();
		sp.excute();
	}

}
