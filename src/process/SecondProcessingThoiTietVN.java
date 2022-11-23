package process;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
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
import dao.staging.DateDimDao;
import dao.staging.ProvinceDimDao;
import dao.staging.RawWeatherFactDao;
import dao.staging.TimeDimdao;
import dao.staging.WeatherFactDao;
import ftp.FTPManager;

public class SecondProcessingThoiTietVN implements Procedure {
	private static final int SOURCE_PROVINCE = 3;
	private static final int SOURCE_VN_ID = 1;

	private static final int SOURCE_VN_EDU_ID = 2;
	private ResultSet rs;

	private FTPManager ftpManager;

	private LogControllerDao logDao;
	private DateDimDao dateDimDao;
	private TimeDimdao timeDimdao;
	private ProvinceDimDao provinceDimDao;
	private WeatherFactDao weatherFactDao;
	private String procedure;
//	private String dateDimId;

	public SecondProcessingThoiTietVN() {
		logDao = new LogControllerDao();
		dateDimDao = new DateDimDao();
		timeDimdao = new TimeDimdao();
		provinceDimDao = new ProvinceDimDao();
		weatherFactDao = new WeatherFactDao();
	}

	private boolean loadProvinceDimIntoStaging() throws SQLException {
		ftpManager = new FTPManager();
		System.out.println("Load provincedim ...");
		Integer logIdProvinceDim = logDao.getIdByStatus(SOURCE_PROVINCE, "EO");
		String destinationProvinceDim = logDao.getDestinationByStatus(SOURCE_PROVINCE);
		if (destinationProvinceDim == null) {
			System.out.println("Have no any provincedim file new in FTP");
			return false;
		}
		try {
			BufferedReader br = ftpManager.getReaderFileInFTPServer(destinationProvinceDim);
			String line;
			while ((line = br.readLine()) != null) {
				provinceDimDao.insert(line);
			}
			logDao.updateStatus(logIdProvinceDim, "EL");
			System.out.println("Provincedim has been loaded into staging!");
			br.close();
			return true;
		} catch (IOException e) {
			System.out.println("Provincedim hasn't been loaded into staging!");
			return false;
		} finally {
			ftpManager.close();
		}
	}

	private boolean loadDateDimIntoStaging() throws IOException, SQLException {
		return dateDimDao.insert();

	}

	private boolean loadTimeDimIntoStaging()
			throws NumberFormatException, FileNotFoundException, SQLException, IOException {
		return timeDimdao.insert();
	}

	private boolean loadWeatherFactIntoStaging() {
		return weatherFactDao.transformFact();
	}

	private boolean isDimLoaded() throws NumberFormatException, FileNotFoundException, SQLException, IOException {
		return loadProvinceDimIntoStaging() && loadDateDimIntoStaging() && loadTimeDimIntoStaging();
	}

	public boolean excute() {
		boolean result = false;
		try {
			if (isDimLoaded()) {
				result = loadWeatherFactIntoStaging();
			} else {
				System.out.println("Load weather fact not successful!");
				return false;
			}
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (result) {
			System.out.println("Load weather fact successfully!");
		}
		return result;
	}

	public static void main(String[] args) throws SQLException, IOException {
		SecondProcessingThoiTietVN sp = new SecondProcessingThoiTietVN();
		sp.excute();
	}
}
