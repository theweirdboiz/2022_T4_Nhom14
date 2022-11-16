package process;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import dao.CreateDateDim;
import dao.CreateTimeDim;
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
//		connection = new MySQLConnection(dbHosting).getConnect();
		sourceConfigDao = new SourceConfigDao();
	}

	public boolean loadWeatherData() {
		boolean result = false;
		// 1. Connect db control
		connection = DbControlConnection.getIntance().getConnect();
		// 2.4 Lấy id có trạng thái 'EO' và ngày ghi log = ngày hôm nay cập nhật sang
		// stt 'EL'
		// 1.1 Lấy một dòng dữ liệu có timeLoad = current_hour and dateLoad =
		// current_date and status ='EO'
		try {
			procedure = Procedure.GET_ONE_FILE_IN_FTP;
			callStmt = connection.prepareCall(procedure);
			callStmt.setInt(1, SOURCE_ID);
			rs = callStmt.executeQuery();
			// 1.1.1 Nếu không có file nào mới -> kết thúc
			if (!rs.next()) {
				System.out.println("Ko co file nao moi");
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
			String fileName = folderName + mySeperator + timeLoad + EXTENSION;

			// 1.1.3 -> vào ftp server và lấy file này xuống
			String path = sourceConfigDao.getDistFolder() + fileSeperator + folderName + fileSeperator + fileName;
			System.out.println(path);
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
					try {
						provinceName = stk.nextToken().trim();
					} catch (NumberFormatException e) {
						provinceName = null;
						continue LOOP;
					}
					try {
						dateLoadData = stk.nextToken().trim();
					} catch (NumberFormatException e) {
						dateLoadData = null;
						continue LOOP;
					}
					try {
						timeLoadData = Integer.parseInt(stk.nextToken().trim().split(":")[0]);
					} catch (NumberFormatException e) {
						timeLoadData = null;
						continue LOOP;
					}
					try {
						currentTemp = Integer.parseInt(stk.nextToken().trim());
					} catch (NumberFormatException e) {
						currentTemp = null;
						continue LOOP;
					}
					try {
						overview = stk.nextToken().trim();

					} catch (NumberFormatException e) {
						overview = null;
						continue LOOP;
					}
					try {
						minTemp = Integer.parseInt(stk.nextToken().trim());

					} catch (NumberFormatException e) {
						minTemp = null;
						continue LOOP;
					}
					try {
						maxTemp = Integer.parseInt(stk.nextToken().trim());

					} catch (NumberFormatException e) {
						maxTemp = null;
						continue LOOP;
					}
					try {
						humidity = Float.parseFloat(stk.nextToken().trim());
					} catch (NumberFormatException e) {
						humidity = null;
						continue LOOP;
					}
					try {
						vision = Float.parseFloat(stk.nextToken().trim());
					} catch (NumberFormatException e) {
						vision = null;
						continue LOOP;
					}
					try {
						wind = Float.parseFloat(stk.nextToken().trim());

					} catch (NumberFormatException e) {
						wind = null;
						continue LOOP;
					}
					try {
						stopPoint = Integer.parseInt(stk.nextToken().trim());

					} catch (NumberFormatException e) {
						stopPoint = null;
						continue LOOP;
					}
					try {
						uvIndex = Float.parseFloat(stk.nextToken().trim());
					} catch (NumberFormatException e) {
						uvIndex = null;
						continue LOOP;
					}
					try {
						airQuality = stk.nextToken().trim();
					} catch (NumberFormatException e) {
						airQuality = null;
						continue LOOP;
					}

					// 1.1.7 Load into staging.raw_weather_data by line
					procedure = Procedure.LOAD_WEATHER_DATA;
					callStmt = connection.prepareCall(procedure);
					callStmt.setInt(1, id);
					callStmt.setString(2, provinceName);
					callStmt.setString(3, dateLoad);
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
				if (result) {
					System.out.println("Load data into staging.raw_weather_data thành công!");
				} else {
					System.out.println("Load data into staging.raw_weather_data không thành công, thử lại nhé!");
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
		String sourceUrl = sourceConfigDao.getURL();
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
		sp.loadWeatherData();
	}
}
