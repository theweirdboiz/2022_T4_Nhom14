package process;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import dao.CreateDateDim;
import dao.CreateTimeDim;
import dao.CurrentTimeStamp;
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
	private final static int SOURCE_ID = 1;
	private final static int SOURCE_DIM_ID = 3;
	private final static String EXTENSION = ".csv";

//2. Transform
	public SecondProcessingThoiTietVN() {
		// 2.1 Connect FTPConfig -> Lấy thông tin FTP Server -> Connect FTP Server
		ftpManager = new FTPManager();
		dbConfigDao = new DbConfigDao();
		dbHosting = dbConfigDao.getStaggingHosting();
		connection = new MySQLConnection(dbHosting).getConnect();
		sourceConfigDao = new SourceConfigDao();
	}

	public void loadWeatherData() {
		connection = DbControlConnection.getIntance().getConnect();
		String result;
		boolean check = false;
		// connect staging
		// download file data csv
		// read by line
		// insert into raw_data
		// 2.2 Lấy source id có trạng thái 'EO' và ngày ghi log = ngày hôm nay
		try {
			procedure = Procedure.CHECK_FILE_CURRENT_IN_FTP_SERVER;
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, SOURCE_ID);
			rs = callStmt.executeQuery();
			if (rs.next()) {
				result = rs.getString("timeLoad");
				String folderName = result.split(" ")[0].trim();
				String ext = ".csv";
				String fileName = folderName + "_" + result.split(" ")[1].split(":")[0].trim() + ext;
				try {
					connection = new MySQLConnection(dbHosting).getConnect();
					String path = sourceConfigDao.getDistFolder(SOURCE_ID) + "/" + folderName + "/" + fileName;
					BufferedReader br = ftpManager.getReaderFileInFTPServer(path);
					String line;
					LOOP: while ((line = br.readLine()) != null) {
						StringTokenizer stk = new StringTokenizer(line, ",");
						int id = Integer.parseInt(stk.nextToken().trim());
						String name = stk.nextToken().trim();
						int currentTemp = Integer.parseInt(stk.nextToken().trim());
						String overview = stk.nextToken().trim();
						int minTemp = Integer.parseInt(stk.nextToken().trim());
						int maxTemp = Integer.parseInt(stk.nextToken().trim());

						float humidity = Float.parseFloat(stk.nextToken().trim());
						float vision = Float.parseFloat(stk.nextToken().trim());
						float wind = Float.parseFloat(stk.nextToken().trim());
						int stopPoint = Integer.parseInt(stk.nextToken().trim());
						float uvIndex = Float.parseFloat(stk.nextToken().trim());
						String airQuality = stk.nextToken().trim();

//					load to staging
						procedure = Procedure.LOAD_WEATHER_DATA;
						callStmt = connection.prepareCall(procedure);
						callStmt.setInt(1, id);
						callStmt.setString(2, name);
						callStmt.setInt(3, currentTemp);
						callStmt.setString(4, overview);
						callStmt.setInt(5, minTemp);
						callStmt.setInt(6, maxTemp);
						callStmt.setFloat(7, humidity);
						callStmt.setFloat(8, vision);
						callStmt.setFloat(9, wind);
						callStmt.setInt(10, stopPoint);
						callStmt.setFloat(11, uvIndex);
						callStmt.setString(12, airQuality);
						int count = callStmt.executeUpdate();
						if (count > 0) {
							check = true;
						}
					}
					if (check) {
						connection = DbControlConnection.getIntance().getConnect();
						procedure = Procedure.FINISH_LOAD_WEATHER_DATA_INTO_STAGING;
						callStmt = connection.prepareCall(procedure);
						callStmt.setInt(1, SOURCE_ID);
						callStmt.setString(2, result);
						rs = callStmt.executeQuery();
						System.out.println("Load raw weather data into staging successful!");
					} else {
						System.out.println("Check again this line!");
					}
					br.close();
				} catch (IOException e) {
					System.out.println("Check loading into staging");
					e.printStackTrace();
				}
			} else {
				System.out.println("Không có file mới nào trên FTP server");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void loadTimeDim() throws IOException {
		boolean checkLoad = false;
		if (CreateTimeDim.create()) {
			try {
				BufferedReader lineReader = new BufferedReader(new FileReader(CreateTimeDim.file));
				String lineText = null;
				// insert new data
				try {

					while ((lineText = lineReader.readLine()) != null) {
						StringTokenizer stk = new StringTokenizer(lineText, ",");
//						String data[] = lineText.split(",");
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
					// TODO Auto-generated catch block
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
			System.out.println("Chưa tạo được file time_dim.csv");
		}
	}

	public void loadDateDim() {
		boolean checkLoad = false;
		if (CreateDateDim.create()) {
			try {
				BufferedReader lineReader = new BufferedReader(new FileReader(CreateDateDim.file));
				String lineText = null;
				procedure = Procedure.DELETE_DATE_DIM;
				// delete all data in datedim
				try {
					callStmt = connection.prepareCall(procedure);
					callStmt.execute();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
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
					// TODO Auto-generated catch block
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

	}

	private void loadProvinceDim() {
		String sourceUrl = sourceConfigDao.getURL(SOURCE_DIM_ID);
		try {
			Document doc = Jsoup.connect(sourceUrl).get();
			Elements provinces = doc.select("table tr:not(:first-child) td:nth-of-type(2) p");
			int count = 1;
			for (Element element : provinces) {
				try {
					procedure = Procedure.LOAD_PROVINCE_DIM;
					callStmt = connection.prepareCall(procedure);
					callStmt.setInt(1, count);
					callStmt.setString(2, element.text());
					callStmt.execute();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				count++;
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void runScript() throws SQLException {
		this.loadDateDim();
		this.loadProvinceDim();
	}

	public static void main(String[] args) throws SQLException, IOException {
		SecondProcessingThoiTietVN sp = new SecondProcessingThoiTietVN();
		sp.loadTimeDim();
		sp.loadDateDim();
//		sp.loadWeatherData();
	}
}
