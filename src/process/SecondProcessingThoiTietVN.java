package process;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.StringTokenizer;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import dao.CreateDateDim;
import dao.CreateTimeDim;
import dao.IdCreater;
import dao.Procedure;
import dao.control.DbConfigDao;
import dao.control.SourceConfigDao;
import db.DbControlConnection;
import db.MySQLConnection;
import ftp.FTPManager;
import model.DbHosting;

public class SecondProcessingThoiTietVN implements Procedure, CreateDateDim {
	private FTPManager ftpManager;
	private CallableStatement callStmt;
	private ResultSet rs;
	private String procedure;
	private Connection connection;
	private DbConfigDao dbConfigDao;
	private DbHosting dbHosting;
	private SourceConfigDao sourceConfigDao;
	private final static int SOURCE_ID_THOI_TIET_VN = 1;
	private final static int SOURCE_ID_THOI_TIET_EDU_VN = 2;
	private final static int SOURCE_ID_PROVINCE_DIM = 3;
	private final static String EXTENSION = ".csv";

//2. Transform
	public SecondProcessingThoiTietVN() {
		// 2.1 Connect FTPConfig -> Lấy thông tin FTP Server -> Connect FTP Server
//		ftpManager = new FTPManager(so);
		dbConfigDao = new DbConfigDao();
		sourceConfigDao = new SourceConfigDao();
	}

	private boolean loadThoiTietVN() {
		boolean result = false;
		ftpManager = new FTPManager(SOURCE_ID_THOI_TIET_VN);
		// 1. Connect db control
		connection = DbControlConnection.getIntance().getConnect();
		// 2.4 Lấy id có trạng thái 'EO' và ngày ghi log = ngày hôm nay cập nhật sang
		// stt 'EL'
		// 1.1 Lấy một dòng dữ liệu
		// current_date and status ='EO' AND ID=SOURCE_ID_THOI_TIET_VN
		try {
			procedure = Procedure.GET_ONE_FILE_IN_FTP;
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, SOURCE_ID_THOI_TIET_VN);
			rs = callStmt.executeQuery();
			// 1.1.1 Nếu không có file nào mới -> kết thúc
			if (!rs.next()) {
				System.out.println("Không có file mới");
				return result;
			}
			// 1.1.2 Nếu có -> lấy thông tin từ dòng dữ liệu này: logId, status, sourceId,
			// timeLoad,dateLoad
			else {
				int logId = rs.getInt("id");
				String dateLoad = rs.getString("dateLoad");
				String timeLoad = rs.getString("timeLoad");
				String mySeperator = "_";
				String fileSeperator = "/";
				String folderName = dateLoad;
				String fileName = "";
				if (Integer.parseInt(timeLoad) < 10) {
					DecimalFormat formatter = new DecimalFormat("00");
					fileName = folderName + mySeperator + formatter.format(Integer.parseInt(timeLoad)) + EXTENSION;
				} else {
					fileName = folderName + mySeperator + timeLoad + EXTENSION;
				}
				// 1.1.3 -> vào ftp server và lấy file này xuống
				String path = sourceConfigDao.getDistFolder(SOURCE_ID_THOI_TIET_VN) + fileSeperator + folderName
						+ fileSeperator + fileName;
				try {
					// 1.1.4 get info db statging
					dbHosting = dbConfigDao.getStagingHosting();
					connection = new MySQLConnection(dbHosting).getConnect();
					// 1.1.5 Mở file
					BufferedReader br = ftpManager.getReaderFileInFTPServer(path);
					String rowData;
					// 1.1.6 Read by line
					LOOP: while ((rowData = br.readLine()) != null) {
						StringTokenizer stk = new StringTokenizer(rowData, ",");
						String provinceName = null;
						String dateLoadData = null;
						Integer timeLoadData = null;
						Integer currentTemp = null;
						String overview = null;
						Integer minTemp = null;
						Integer maxTemp = null;
						Float humidity = null;
						Float vision = null;
						Float wind = null;
						Integer stopPoint = null;
						Float uvIndex = null;
						String airQuality = null;

						int id = Integer.parseInt(stk.nextToken().trim());
						provinceName = stk.nextToken().trim();
						dateLoadData = stk.nextToken().trim();
						timeLoadData = Integer.parseInt(stk.nextToken().trim().split(":")[0]);
						currentTemp = Integer.parseInt(stk.nextToken().trim());
						overview = stk.nextToken().trim();
						minTemp = Integer.parseInt(stk.nextToken().trim());
						maxTemp = Integer.parseInt(stk.nextToken().trim());
						humidity = Float.parseFloat(stk.nextToken().trim());
						vision = Float.parseFloat(stk.nextToken().trim());
						wind = Float.parseFloat(stk.nextToken().trim());
						stopPoint = Integer.parseInt(stk.nextToken().trim());
						uvIndex = Float.parseFloat(stk.nextToken().trim());
						airQuality = stk.nextToken().trim();

						// 1.1.7 Load into staging.raw_weather_data by line

						procedure = Procedure.LOAD_THOI_TIET_VN_INTO_STAGING;
						callStmt = connection.prepareCall(procedure);
						callStmt.setInt(1, id);
						callStmt.setString(2, provinceName);
						callStmt.setString(3, dateLoadData);
						callStmt.setInt(4, timeLoadData);
						callStmt.setInt(5, currentTemp);
						callStmt.setString(6, overview);
						callStmt.setInt(7, minTemp);
						callStmt.setInt(8, maxTemp);
						callStmt.setFloat(9, humidity);
						callStmt.setFloat(10, vision);
						callStmt.setFloat(11, wind);
						callStmt.setInt(12, stopPoint);
						callStmt.setFloat(13, uvIndex);
						callStmt.setString(14, airQuality);
						result = callStmt.executeUpdate() > 0;
					}
					// Kiểm tra kết quả quá trình load
					connection = DbControlConnection.getIntance().getConnect();
					if (result) {
						// Update stt EL
						procedure = Procedure.UPDATE_STATUS;
						callStmt = connection.prepareCall(procedure);
						callStmt.setString(1, "EL");
						callStmt.setInt(2, logId);
						callStmt.execute();
						System.out.println("Load data into staging.raw_weather_data thành công!");
						return true;
					} else {
						procedure = Procedure.UPDATE_STATUS;
						callStmt = connection.prepareCall(procedure);
						callStmt.setString(1, "EF");
						callStmt.setInt(2, logId);
						callStmt.execute();
						System.out.println("Load data into staging.raw_weather_data ko thành công!");
						return false;
					}
				} catch (IOException e) {
					e.printStackTrace();
					System.out.println("Chắc là đường dẫn này không tồn tại!");
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	private boolean loadProvinceDim() {
		boolean result = false;
		ftpManager = new FTPManager(SOURCE_ID_PROVINCE_DIM);
		connection = DbControlConnection.getIntance().getConnect();

		try {
			procedure = Procedure.GET_ONE_FILE_IN_FTP;
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, SOURCE_ID_THOI_TIET_EDU_VN);
			rs = callStmt.executeQuery();
			// 1.1.1 Nếu không có file nào mới -> kết thúc
			if (!rs.next()) {
				System.out.println("Không có file mới");
				return result;
			}
			// 1.1.2 Nếu có -> lấy thông tin từ dòng dữ liệu này: logId, status, sourceId,
			// timeLoad,dateLoad
			int logId = rs.getInt("id");
			String dateLoad = rs.getString("dateLoad");
			String timeLoad = rs.getString("timeLoad");
			String mySeperator = "_";
			String fileSeperator = "/";
			String folderName = dateLoad;
			String fileName = "";
			if (Integer.parseInt(timeLoad) < 10) {
				DecimalFormat formatter = new DecimalFormat("00");
				fileName = folderName + mySeperator + formatter.format(Integer.parseInt(timeLoad)) + EXTENSION;
			} else {
				fileName = folderName + mySeperator + timeLoad + EXTENSION;
			}
			// 1.1.3 -> vào ftp server và lấy file này xuống
			String path = sourceConfigDao.getDistFolder(SOURCE_ID_THOI_TIET_EDU_VN) + fileSeperator + folderName
					+ fileSeperator + fileName;
			try {
				// 1.1.4 get info db statging
				dbHosting = dbConfigDao.getStagingHosting();
				connection = new MySQLConnection(dbHosting).getConnect();
				// 1.1.5 Mở file
				BufferedReader br = ftpManager.getReaderFileInFTPServer(path);
				String rowData;
				// 1.1.6 Read by line
				LOOP: while ((rowData = br.readLine()) != null) {
					StringTokenizer stk = new StringTokenizer(rowData, ",");
					String provinceName = null;
					int id = Integer.parseInt(stk.nextToken().trim());
					provinceName = stk.nextToken().trim();

					// 1.1.7 Load into staging.raw_weather_data by line
					procedure = Procedure.LOAD_PROVINCE_DIM;
					callStmt = connection.prepareCall(procedure);
					callStmt.setInt(1, id);
					callStmt.setString(2, provinceName);

					result = callStmt.executeUpdate() > 0;
				}
				// Kiểm tra kết quả quá trình load
				connection = DbControlConnection.getIntance().getConnect();
				if (result) {
					// Update stt EL
					procedure = Procedure.UPDATE_STATUS;
					callStmt = connection.prepareCall(procedure);
					callStmt.setString(1, "EL");
					callStmt.setInt(2, logId);
					callStmt.execute();
					System.out.println("Load data into staging.provinceDim thành công!");
					connection.close();
					return true;
				} else {
					procedure = Procedure.UPDATE_STATUS;
					callStmt = connection.prepareCall(procedure);
					callStmt.setString(1, "EF");
					callStmt.setInt(2, logId);
					callStmt.execute();
					System.out.println("Load data into staging.provinceDim ko thành công!");
					connection.close();
					return false;
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Chắc là đường dẫn này không tồn tại!");
				connection.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return result;
	}

	private boolean loadThoiTietEduVN() {
		boolean result = false;
		ftpManager = new FTPManager(SOURCE_ID_THOI_TIET_EDU_VN);
		// 1. Connect db control
		connection = DbControlConnection.getIntance().getConnect();
		// 2.4 Lấy id có trạng thái 'EO' và ngày ghi log = ngày hôm nay cập nhật sang
		// stt 'EL'
		// 1.1 Lấy một dòng dữ liệu
		// current_date and status ='EO' AND ID=SOURCE_ID_THOI_TIET_VN
		try {
			procedure = Procedure.GET_ONE_FILE_IN_FTP;
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, SOURCE_ID_THOI_TIET_EDU_VN);
			rs = callStmt.executeQuery();
			// 1.1.1 Nếu không có file nào mới -> kết thúc
			if (!rs.next()) {
				System.out.println("Không có file mới");
				return result;
			}
			// 1.1.2 Nếu có -> lấy thông tin từ dòng dữ liệu này: logId, status, sourceId,
			// timeLoad,dateLoad
			int logId = rs.getInt("id");
			String dateLoad = rs.getString("dateLoad");
			String timeLoad = rs.getString("timeLoad");
			String mySeperator = "_";
			String fileSeperator = "/";
			String folderName = dateLoad;
			String fileName = "";
			if (Integer.parseInt(timeLoad) < 10) {
				DecimalFormat formatter = new DecimalFormat("00");
				fileName = folderName + mySeperator + formatter.format(Integer.parseInt(timeLoad)) + EXTENSION;
			} else {
				fileName = folderName + mySeperator + timeLoad + EXTENSION;
			}
			// 1.1.3 -> vào ftp server và lấy file này xuống
			String path = sourceConfigDao.getDistFolder(SOURCE_ID_THOI_TIET_EDU_VN) + fileSeperator + folderName
					+ fileSeperator + fileName;
			try {
				// 1.1.4 get info db statging
				dbHosting = dbConfigDao.getStagingHosting();
				connection = new MySQLConnection(dbHosting).getConnect();
				// 1.1.5 Mở file
				BufferedReader br = ftpManager.getReaderFileInFTPServer(path);
				String rowData;
				// 1.1.6 Read by line
				LOOP: while ((rowData = br.readLine()) != null) {
					StringTokenizer stk = new StringTokenizer(rowData, ",");

					String provinceName = null;
					String dateLoadData = null;
					Integer timeLoadData = null;
					Integer currentTemp = null;
					String overview = null;
					Integer minTemp = null;
					Integer maxTemp = null;
					Float humidity = null;
					Float vision = null;
					Float wind = null;
					Integer stopPoint = null;
					Float uvIndex = null;
					String airQuality = null;

					int id = Integer.parseInt(stk.nextToken().trim());
					provinceName = stk.nextToken().trim();
					dateLoadData = stk.nextToken().trim();
					timeLoadData = Integer.parseInt(stk.nextToken().trim().split(":")[0]);
					currentTemp = Integer.parseInt(stk.nextToken().trim());
					overview = stk.nextToken().trim();
					minTemp = Integer.parseInt(stk.nextToken().trim());
					maxTemp = Integer.parseInt(stk.nextToken().trim());
					humidity = Float.parseFloat(stk.nextToken().trim());
					vision = Float.parseFloat(stk.nextToken().trim());
					wind = Float.parseFloat(stk.nextToken().trim());
					stopPoint = Integer.parseInt(stk.nextToken().trim());
					uvIndex = Float.parseFloat(stk.nextToken().trim());
					airQuality = stk.nextToken().trim();
					// 1.1.7 Load into staging.raw_weather_data by line
					procedure = Procedure.LOAD_THOI_TIET_EDU_VN_INTO_STAGING;
					callStmt = connection.prepareCall(procedure);
					callStmt.setInt(1, id);
					callStmt.setString(2, provinceName);
					callStmt.setString(3, dateLoadData);
					callStmt.setInt(4, timeLoadData);
					callStmt.setInt(5, currentTemp);
					callStmt.setString(6, overview);
					callStmt.setInt(7, minTemp);
					callStmt.setInt(8, maxTemp);
					callStmt.setFloat(9, humidity);
					callStmt.setFloat(10, vision);
					callStmt.setFloat(11, wind);
					callStmt.setInt(12, stopPoint);
					callStmt.setFloat(13, uvIndex);
					callStmt.setString(14, airQuality);
					result = callStmt.executeUpdate() > 0;
				}
				// Kiểm tra kết quả quá trình load
				connection = DbControlConnection.getIntance().getConnect();
				if (result) {
					// Update stt EL
					procedure = Procedure.UPDATE_STATUS;
					callStmt = connection.prepareCall(procedure);
					callStmt.setString(1, "EL");
					callStmt.setInt(2, logId);
					callStmt.execute();
					System.out.println("Load data into staging.raw_weather_data thành công!");
					return true;
				} else {
					procedure = Procedure.UPDATE_STATUS;
					callStmt = connection.prepareCall(procedure);
					callStmt.setString(1, "EF");
					callStmt.setInt(2, logId);
					callStmt.execute();
					System.out.println("Load data into staging.raw_weather_data ko thành công!");
					return false;
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Chắc là đường dẫn này không tồn tại!");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	public boolean loadTimeDim() throws IOException {
		dbHosting = dbConfigDao.getStagingHosting();
		connection = new MySQLConnection(dbHosting).getConnect();
		boolean checkLoad = false;
		// Kiểm tra datedim đã tồn tại trong staging chưa?
		procedure = Procedure.CHECK_TIME_DIM_IS_EXISTED;
		boolean checkIsExisted = false;
		try {
			callStmt = connection.prepareCall(procedure);
			rs = callStmt.executeQuery();
			checkIsExisted = rs.next();
		} catch (SQLException e) {
			System.out.println("Có lỗi ở CHECK_TIME_DIM_IS_EXISTED");
			e.printStackTrace();
		}
		// Nếu đã tồn tại thì kết thúc luôn
		if (checkIsExisted) {
			System.out.println("timedim đã được load trước đó");
			return true;
		}
		// Nếu chưa, tạo mới rồi insert vào staging
		if (CreateTimeDim.create()) {
			try {
				BufferedReader lineReader = new BufferedReader(new FileReader(CreateTimeDim.file));
				String lineText = null;
				// insert new data
				try {
					while ((lineText = lineReader.readLine()) != null) {
						StringTokenizer stk = new StringTokenizer(lineText, ",");
						try {
							procedure = Procedure.LOAD_TIME_DIM;
							callStmt = connection.prepareCall(procedure);
							callStmt.setInt(1, Integer.parseInt(stk.nextToken()));
							callStmt.setString(2, stk.nextToken());
							int result = callStmt.executeUpdate();
							if (result > 0) {
								checkLoad = true;
							}
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
				} catch (NumberFormatException e) {
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				try {
					lineReader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (FileNotFoundException e) {
				try {
					connection.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				e.printStackTrace();
			}
			if (checkLoad) {
				System.out.println("Thêm dữ liệu timedim thành công!");
			} else {
				System.out.println("Thêm dữ liệu timedim thất bại!");
			}
		} else {
			System.out.println("Chưa tạo được file time_dim.csv!");
		}
		try {
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return checkLoad;

	}

	public boolean loadDateDim() {
		dbHosting = dbConfigDao.getStagingHosting();
		connection = new MySQLConnection(dbHosting).getConnect();
		boolean checkLoad = false;
		// Kiểm tra datedim đã tồn tại trong staging chưa?
		procedure = Procedure.CHECK_DATE_DIM_IS_EXISTED;
		boolean checkIsExisted = false;
		try {
			callStmt = connection.prepareCall(procedure);
			rs = callStmt.executeQuery();
			checkIsExisted = rs.next();
		} catch (SQLException e) {
			System.out.println("Có lỗi ở CHECK_DATE_DIM_IS_EXISTED");
			e.printStackTrace();
		}
		// Nếu đã tồn tại thì kết thúc luôn
		if (checkIsExisted) {
			System.out.println("datedim đã được load trước đó");
			return true;
		}
		// Nếu chưa, tạo mới rồi insert vào staging
		if (CreateDateDim.create()) {
			try {
				BufferedReader lineReader = new BufferedReader(new FileReader(CreateDateDim.file));
				String lineText = null;
				// insert new data
				try {
					while ((lineText = lineReader.readLine()) != null) {
						String[] data = lineText.split(",");
						int id = Integer.parseInt(data[0].trim());
						String date = data[1].trim();
						int year = Integer.parseInt(data[2].trim());
						int month = Integer.parseInt(data[3].trim());
						int day = Integer.parseInt(data[4].trim());
						String dayOfWeek = data[5].trim();
						try {
							procedure = Procedure.LOAD_DATE_DIM;
							callStmt = connection.prepareCall(procedure);
							callStmt.setInt(1, id);
							callStmt.setString(2, date);
							callStmt.setInt(3, year);
							callStmt.setInt(4, month);
							callStmt.setInt(5, day);
							callStmt.setString(6, dayOfWeek);
							int result = callStmt.executeUpdate();
							if (result > 0) {
								checkLoad = true;
							}
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
				} catch (NumberFormatException e) {
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				try {
					lineReader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (FileNotFoundException e) {
				try {
					connection.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				e.printStackTrace();
			}
			if (checkLoad) {
				System.out.println("Thêm dữ liệu datedim thành công!");
			} else {
				System.out.println("Thêm dữ liệu datedim thất bại!");
			}
		} else {
			System.out.println("Chưa tạo được file date_dim.csv!");
		}
		try {
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return checkLoad;
	}

	public boolean isDimLoaded() throws IOException {
		return loadTimeDim() && loadDateDim();
	}

	private boolean loadWeatherData() {
		boolean result = false;
		dbHosting = dbConfigDao.getStagingHosting();
		connection = new MySQLConnection(dbHosting).getConnect();
		procedure = Procedure.LOAD_WEATHER_DATA;
		try {
			callStmt = connection.prepareCall(procedure);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	private boolean transformWeatherFact() {
		boolean result = false;
		dbHosting = dbConfigDao.getStagingHosting();
		connection = new MySQLConnection(dbHosting).getConnect();
		String query = "SELECT * FROM weatherfact";
		try {
			PreparedStatement ps = connection.prepareStatement(query);
			rs = ps.executeQuery();
			while (rs.next()) {
				procedure = Procedure.TRANSFORM_WEATHER_FACT;
				callStmt = connection.prepareCall(procedure);
				callStmt.setInt(1, IdCreater.createIdByCurrentTime());
				callStmt.setInt(2, rs.getInt("id"));
				callStmt.setInt(3, rs.getInt("province_id"));
				callStmt.setInt(4, rs.getInt("date_id"));
				callStmt.setInt(5, rs.getInt("time_id"));
				callStmt.setInt(6, rs.getInt("currentTemp"));
				callStmt.setString(7, rs.getString("overview"));
				callStmt.setInt(8, rs.getInt("lowestTemp"));
				callStmt.setInt(9, rs.getInt("maximumTemp"));
				callStmt.setInt(10, rs.getInt("humidity"));
				callStmt.setFloat(11, rs.getFloat("vision"));
				callStmt.setFloat(12, rs.getFloat("wind"));
				callStmt.setInt(13, rs.getInt("stopPoint"));
				callStmt.setInt(14, rs.getInt("uvIndex"));
				callStmt.setString(15, rs.getString("airQuality"));
				result = callStmt.execute();
			}
//			callStmt = connection.prepareCall(procedure);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		return result;
	}

	public boolean runScript() throws SQLException, IOException {
		boolean result = false;
		// Kiểm tra các dim đã load vào staging hay chưa?
		if (isDimLoaded()) {
			if (loadThoiTietVN() && loadThoiTietEduVN() && loadProvinceDim()) {
				result = loadWeatherData();
			}
		} else {
			result = false;
		}
		if (result) {
			transformWeatherFact();
		}
		return result;
	}

	public static void main(String[] args) throws SQLException, IOException {
		SecondProcessingThoiTietVN sp = new SecondProcessingThoiTietVN();
		sp.transformWeatherFact();
//		sp.loadTimeDim();
//		sp.loadThoiTietEduVN();
//		sp.loadThoiTietVN();
//		sp.runScript();
	}
}
