package process;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.SQLException;

import dao.CreateDateDim;
import dao.IdCreater;
import dao.Procedure;
import dao.control.LogControllerDao;
import dao.stagging.DateDimDao;
import dao.stagging.ProvinceDimDao;
import dao.stagging.WeatherFactDao;
import ftp.FTPManager;

public class SecondProcessing implements Procedure, CreateDateDim {
	
	private static final String SOURCE_THOITIETVN_ID = "1";
	private static final String SOURCE_THOITIETEDUVN_ID = "2";
	private static final String SOURCE_PROVINCE = "3";

	private FTPManager ftpManager;
	private LogControllerDao logDao;
	private DateDimDao dateDimDao;
	private ProvinceDimDao provinceDimDao;
	private WeatherFactDao weatherFactDao;
	
	private String dateDimId;
	
	public SecondProcessing() {
		
		logDao = new LogControllerDao();
		dateDimDao = new DateDimDao();
		provinceDimDao = new ProvinceDimDao();
		weatherFactDao = new WeatherFactDao();
	}
	
	public void execute() throws InterruptedException {
		loadProvinceToStagging();
		loadWeatherFactFromThoiTietVN();
		loadWeatherFactFromThoiTietEduVN();
	}
	
	public boolean loadProvinceToStagging() {
		ftpManager = new FTPManager();
		System.out.println("Start load province...");
		String provinceFtpPath = logDao.getPathFTPWithStatusInLog("EO", SOURCE_PROVINCE);
		if (provinceFtpPath == null) {
			System.out.println("There aren't province need load");
			return false;
		}

		try {
			BufferedReader provinceReader = ftpManager.getReaderFileInFTPServer(provinceFtpPath);
			
			String line;
			while((line = provinceReader.readLine()) != null) {
				provinceDimDao.insert(line);
			}
			
			logDao.setStatusByFTPPath(provinceFtpPath, "EL");
			System.out.println("Sussessful load province");
			provinceReader.close();
			return true;
		} catch (IOException e) {
			logDao.setStatusByFTPPath(provinceFtpPath, "EF");
			System.out.println("Load province Fail");
			return false;
		}finally {
			ftpManager.close();
		}
	}
	
	public boolean loadWeatherFactFromThoiTietVN() {
		ftpManager = new FTPManager();
		System.out.println("Start load thoitietvn...");
		String weatherPath = logDao.getPathFTPWithStatusInLog("EO", SOURCE_THOITIETVN_ID);
		
		if (weatherPath == null) {
			System.out.println("There aren't weather from thoitietvn need load");
			return false;
		}
		
		System.out.println(weatherPath);
		
		try {
			BufferedReader provinceReader = ftpManager.getReaderFileInFTPServer(weatherPath);
			dateDimId = IdCreater.createIdRandom();
			String line = provinceReader.readLine();
			dateDimDao.insert(dateDimId, line);
			while((line = provinceReader.readLine()) != null) {
				String provinceId = provinceDimDao.getIdByProvinceName(line.split(",")[1].trim());
				if (provinceId != null) weatherFactDao.insert(dateDimId, provinceId, line);
			}
			
			logDao.setStatusByFTPPath(weatherPath, "EL");
			System.out.println("Sussessful load weather from thoitietvn");
			provinceReader.close();
			return true;
		} catch (IOException e) {
			logDao.setStatusByFTPPath(weatherPath, "EF");
			System.out.println("Load province Fail");
			return false;
		}finally {
			ftpManager.close();
		}
	}
	
	public boolean loadWeatherFactFromThoiTietEduVN() {
		ftpManager = new FTPManager();
		System.out.println("Start load thoitieteduvn...");
		String weatherPath = logDao.getPathFTPWithStatusInLog("EO", SOURCE_THOITIETEDUVN_ID);
		
		System.out.println(weatherPath);
		if (weatherPath == null) {
			System.out.println("There aren't weather from thoitieteduvn need load");
			return false;
		}
		
		try {
			BufferedReader provinceReader = ftpManager.getReaderFileInFTPServer(weatherPath);
			
			String line = provinceReader.readLine();
			if (dateDimId == null) {
				dateDimId = IdCreater.createIdRandom();
				dateDimDao.insert(dateDimId, line);
			}
			while((line = provinceReader.readLine()) != null) {
				String provinceId = provinceDimDao.getIdByProvinceName(line.split(",")[1]);
				
				if (provinceId != null && !weatherFactDao.checkExistsProvince(provinceId))
					weatherFactDao.insert(dateDimId, provinceId, line);
			}
			
			logDao.setStatusByFTPPath(weatherPath, "EL");
			System.out.println("Sussessful load from thoitieteduvn");
			provinceReader.close();
			return true;
		} catch (IOException e) {
			logDao.setStatusByFTPPath(weatherPath, "EF");
			System.out.println("Load province Fail");
			return false;
		}finally {
			ftpManager.close();
		}
	}
	
//	private FTPManager ftpManager;
//	private CallableStatement callStmt;
//	private ResultSet rs;
//	private String procedure;
//	private Connection connection;
//	private DbConfigDao dbConfigDao;
//	private DbHosting dbHosting;
//	private SourceConfigDao sourceConfigDao;
//	private final static int SOURCE_ID = 1;
//	private final static int SOURCE_DIM_ID = 3;
//	private final static String EXTENSION = ".csv";
//
////2. Transform
//	public SecondProcessingThoiTietVN() {
//		// 2.1 Connect FTPConfig -> Lấy thông tin FTP Server -> Connect FTP Server
//		ftpManager = new FTPManager();
//		dbConfigDao = new DbConfigDao();
//		dbHosting = dbConfigDao.getStaggingHosting();
//		connection = new MySQLConnection(dbHosting).getConnect();
//		sourceConfigDao = new SourceConfigDao();
//	}
//
//	public void loadWeatherData() {
//		connection = DbControlConnection.getIntance().getConnect();
//		String result;
//		boolean check = false;
//		// connect staging
//		// download file data csv
//		// read by line
//		// insert into raw_data
//		// 2.2 Lấy source id có trạng thái 'EO' và ngày ghi log = ngày hôm nay
//		try {
//			procedure = Procedure.CHECK_FILE_CURRENT_IN_FTP_SERVER;
//			callStmt = connection.prepareCall(procedure);
//			callStmt.setInt(1, SOURCE_ID);
//			rs = callStmt.executeQuery();
//			if (rs.next()) {
//				result = rs.getString("timeLoad");
//				String folderName = result.split(" ")[0].trim();
//				String ext = ".csv";
//				String fileName = folderName + "_" + result.split(" ")[1].split(":")[0].trim() + ext;
//				try {
//					connection = new MySQLConnection(dbHosting).getConnect();
//					String path = sourceConfigDao.getDistFolder(SOURCE_ID) + "/" + folderName + "/" + fileName;
//					BufferedReader br = ftpManager.getReaderFileInFTPServer(path);
//					String line;
//					LOOP: while ((line = br.readLine()) != null) {
//						StringTokenizer stk = new StringTokenizer(line, ",");
//						int id = Integer.parseInt(stk.nextToken().trim());
//						String name = stk.nextToken().trim();
//						int currentTemp = Integer.parseInt(stk.nextToken().trim());
//						String overview = stk.nextToken().trim();
//						int minTemp = Integer.parseInt(stk.nextToken().trim());
//						int maxTemp = Integer.parseInt(stk.nextToken().trim());
//
//						float humidity = Float.parseFloat(stk.nextToken().trim());
//						float vision = Float.parseFloat(stk.nextToken().trim());
//						float wind = Float.parseFloat(stk.nextToken().trim());
//						int stopPoint = Integer.parseInt(stk.nextToken().trim());
//						float uvIndex = Float.parseFloat(stk.nextToken().trim());
//						String airQuality = stk.nextToken().trim();
//
////					load to staging
//						procedure = Procedure.LOAD_WEATHER_DATA;
//						callStmt = connection.prepareCall(procedure);
//						callStmt.setInt(1, id);
//						callStmt.setString(2, name);
//						callStmt.setInt(3, currentTemp);
//						callStmt.setString(4, overview);
//						callStmt.setInt(5, minTemp);
//						callStmt.setInt(6, maxTemp);
//						callStmt.setFloat(7, humidity);
//						callStmt.setFloat(8, vision);
//						callStmt.setFloat(9, wind);
//						callStmt.setInt(10, stopPoint);
//						callStmt.setFloat(11, uvIndex);
//						callStmt.setString(12, airQuality);
//						int count = callStmt.executeUpdate();
//						if (count > 0) {
//							check = true;
//						}
//					}
//					if (check) {
//						connection = DbControlConnection.getIntance().getConnect();
//						procedure = Procedure.FINISH_LOAD_WEATHER_DATA_INTO_STAGING;
//						callStmt = connection.prepareCall(procedure);
//						callStmt.setInt(1, SOURCE_ID);
//						callStmt.setString(2, result);
//						rs = callStmt.executeQuery();
//						System.out.println("Load raw weather data into staging successful!");
//					} else {
//						System.out.println("Check again this line!");
//					}
//					br.close();
//				} catch (IOException e) {
//					System.out.println("Check loading into staging");
//					e.printStackTrace();
//				}
//			} else {
//				System.out.println("Không có file mới nào trên FTP server");
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//	}
//
//	public void loadTimeDim() {
//		try {
//			CreateTimeDim.create();
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//	}
//
//	public void loadDateDim() {
//		if (CreateDateDim.create()) {
//			try {
//				BufferedReader lineReader = new BufferedReader(new FileReader(CreateDateDim.file));
//				String lineText = null;
//				procedure = Procedure.DELETE_DATE_DIM;
//				// delete all data in datedim
//				try {
//					callStmt = connection.prepareCall(procedure);
//					callStmt.execute();
//				} catch (SQLException e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//				}
//				// insert new data
//				try {
//					while ((lineText = lineReader.readLine()) != null) {
//						String[] data = lineText.split(",");
//						int id = Integer.parseInt(data[0].trim());
//						String date = data[1].trim();
//						int year = Integer.parseInt(data[2].trim());
//						int month = Integer.parseInt(data[3].trim());
//						int day = Integer.parseInt(data[4].trim());
//						String dayOfWeek = data[5].trim();
//
//						System.out.println(id + " " + date + " " + year + " " + month + " " + day + " " + dayOfWeek);
//						try {
//							procedure = Procedure.LOAD_DATE_DIM;
//							callStmt = connection.prepareCall(procedure);
//							callStmt.setInt(1, id);
//							callStmt.setString(2, date);
//							callStmt.setInt(3, year);
//							callStmt.setInt(4, month);
//							callStmt.setInt(5, day);
//							callStmt.setString(6, dayOfWeek);
//							callStmt.execute();
//						} catch (SQLException e) {
//							e.printStackTrace();
//						}
//					}
//				} catch (NumberFormatException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//
//				try {
//					lineReader.close();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			} catch (FileNotFoundException e) {
//				try {
//					connection.rollback();
//				} catch (SQLException e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//				}
//				e.printStackTrace();
//			}
//		}
//
//	}
//
//	private void loadProvinceDim() {
//		String sourceUrl = sourceConfigDao.getURL(SOURCE_DIM_ID);
//		try {
//			Document doc = Jsoup.connect(sourceUrl).get();
//			Elements provinces = doc.select("table tr:not(:first-child) td:nth-of-type(2) p");
//			int count = 1;
//			for (Element element : provinces) {
//				try {
//					procedure = Procedure.LOAD_PROVINCE_DIM;
//					callStmt = connection.prepareCall(procedure);
//					callStmt.setInt(1, count);
//					callStmt.setString(2, element.text());
//					callStmt.execute();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//				count++;
//			}
//
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
//	}
//
//	public boolean loadData() {
//		// Vào log, kiểm tra có file mới được upload lên FTP server hay không bằng cách
//		// check status = EO?
//		try {
//			procedure = Procedure.IS_EXISTED;
//			callStmt = connection.prepareCall(procedure);
//			callStmt.setInt(1, SOURCE_ID);
//			rs = callStmt.executeQuery();
//			boolean isExisted = false;
//			if (rs.next()) {
//				isExisted = rs.getInt(1) > 0 ? true : false;
//			}
//			// Nếu có
//			// Lấy một dòng dữ liệu, lấy timeLoad từ dòng đó -> tìm đến folder trên FTP theo
//			// timeLoad
//			String dateLoad = sourceConfigDao.getTimeLoad(SOURCE_ID).split(" ")[0];
//			String hourLoad = sourceConfigDao.getTimeLoad(SOURCE_ID).split(" ")[1].split(":")[0];
//			String path = sourceConfigDao.getDistFolder(SOURCE_ID) + "/" + dateLoad + "/"
//					+ CurrentTimeStamp.getCurrentDate(dateLoad, hourLoad + EXTENSION);
//			System.out.println(path);
//			try {
//				System.out.println(path);
//				BufferedReader br = ftpManager.getReaderFileInFTPServer(path);
//				String line;
//				while ((line = br.readLine()) != null) {
//					System.out.println(line);
//				}
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//
//		// download file
//
//		// insert by row into statging
//
////		Nếu không -> Kết thúc
//		return false;
//	}
//
//	public void runScript() throws SQLException {
//		this.loadDateDim();
//		this.loadProvinceDim();
//
//	}

	public static void main(String[] args) throws SQLException, IOException, InterruptedException {
		SecondProcessing sp = new SecondProcessing();
		sp.execute();
//		sp.loadTimeDim();
//		sp.loadWeatherData();
	}
}
