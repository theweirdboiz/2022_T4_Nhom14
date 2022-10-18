package process_1;

import connect_database.ConnectDatabase;
import model.Source;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class OneProcess {
    private final String WEB_URL = "https://thoitiet.vn";
    private ArrayList<Source> sources;
    ConnectDatabase connectDatabase;

    public void loadDimSources() throws SQLException {
        Document doc = null;
        try {
            doc = Jsoup.connect(WEB_URL).get();
            sources = new ArrayList<Source>();
            sources.add(new Source(1, "source_1", "https://thoitiet.vn/"));
            sources.add(new Source(2, "source_2", "https://thoitiet.edu.vn/"));
            for (int i = 0; i < sources.size(); i++) {
                String query = "INSERT INTO config VALUES(?,?,?)";
                PreparedStatement ps = connectDatabase.getConn().prepareStatement(query);
                ps.setInt(1, sources.get(i).getId());
                ps.setString(2, sources.get(i).getName());
                ps.setString(3, sources.get(i).getUrl());
                ps.execute();
            }
            connectDatabase.closeConnecting();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //    1. connect db control
    public void connectDatabase(String databaseName) throws SQLException, ClassNotFoundException {
        connectDatabase = new ConnectDatabase();
        connectDatabase.connecting(databaseName);
    }

    // 2. get 1 row from config table where id= ?
    public void insert_into_log() throws SQLException {
        String query = "SELECT * FROM config";
        Statement stmt = connectDatabase.getConn().createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            int id = rs.getInt("ID");
            //3. Call procedure
            String insertLogQuery = "{CALL INSERT_INTO_LOG(?,?,?)}";
            //4. Update date -> finish
            SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
            Date date = new Date();
            CallableStatement callStmt = connectDatabase.getConn().prepareCall(insertLogQuery);
            callStmt.setInt(1, id);
            callStmt.setString(2, formatter.format(date));
            callStmt.setString(3, "ER");
            callStmt.execute();
        }
    }

    //5. Extract data successful? update status = EO : update status = EF where id= ?
    public void extractData() throws SQLException {
        String query = "SELECT DISTINCT(*) FROM log WHERE STATUS='ER'";
        Statement stmt = connectDatabase.getConn().createStatement();
        ResultSet rs = stmt.executeQuery(query);

        while (rs.next()) {
            String time = rs.getString("TIME");
            int id = rs.getInt("CONFIG_ID");
            query = "SELECT URL from config WHERE ID=?";

            PreparedStatement ps = connectDatabase.getConn().prepareStatement(query);
            ps.setInt(1, id);
            rs = ps.executeQuery();

            File sourceFile = new File("./data" + id + ".csv");


            String url = rs.getString("url");
            try {
                Document doc = Jsoup.connect(url).get();
                doc.outputSettings().charset("UTF-8");

                PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(sourceFile), StandardCharsets.UTF_8));
                Elements provinces = null;
                if (url == "https://thoitiet.vn/") {
                    provinces = doc.select(".megamenu a");

                }
                if (url == "https://thoitiet.edu.vn/") {
                    provinces = doc.select(".card-body h3 > a");
                }
                for (int i = 0; i < provinces.size(); i++) {
                    String dataURL = WEB_URL + provinces.get(i).attr("href");
                    Document docItem = Jsoup.connect(dataURL).get();
                    // province
                    writer.write(provinces.get(i).attr("title") + ",");
                    // current_time
                    writer.write(docItem.select("#timer").text().replace("| ", "") + ",");
                    Element currentTemp = docItem.select(".current-temperature").first();
                    // current_temperature
                    writer.write(currentTemp.text() + ",");
                    // overview
                    writer.write(docItem.select(".overview-caption-item.overview-caption-item-detail").text() + ","); // lowest_temp
                    // lowest
                    writer.write(
                            docItem.select(".text-white.op-8.fw-bold:first-of-type").text().split("/")[0] + ",");
                    // maximum_temp
                    writer.write(
                            docItem.selectFirst(".weather-detail .text-white.op-8.fw-bold:first-child").text().split("/")[1] + ",");
                    // humidity
                    writer.write(docItem.select(".weather-detail .text-white.op-8.fw-bold").get(1).text() + ",");
                    // vision
                    writer.write(docItem.select(".weather-detail .text-white.op-8.fw-bold").get(2).text() + ",");
                    // wind
                    writer.write(docItem.select(".weather-detail .text-white.op-8.fw-bold").get(3).text() + ",");
                    // stop_point
                    writer.write(docItem.select(".weather-detail .text-white.op-8.fw-bold").get(4).text() + ",");
                    // uv_index
                    writer.write(docItem.select(".weather-detail .text-white.op-8.fw-bold").get(5).text() + ",");
                    // air_quality
                    writer.write(docItem.select(".air-api.air-active").text() + ",");
                    // time_refresh
                    writer.write(docItem.select(".location-auto-refresh").text() + "\n");
                }
                writer.write("");
                writer.flush();
                writer.close();
                // save as FTP server
                // OK ? update EO : Update EF
                query = "UPDATE log SET STATUS =? WHERE CONFIG_ID=? AND TIME=?";
                ps = connectDatabase.getConn().prepareStatement(query);
                if (sourceFile.length() > 0) {
                    //save FTP
                    ps.setString(1, "EO");
                } else {
                    ps.setString(1, "EF");
                }
                ps.setInt(2, id);
                ps.setString(3, time);
                ps.execute();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        OneProcess oneProcess = new OneProcess();
        oneProcess.connectDatabase("control");
//        oneProcess.loadDimSources();
        oneProcess.insert_into_log();
        oneProcess.extractData();
    }
}
