import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import java.sql.*;
import java.beans.PropertyVetoException;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.MultiMap;
import org.vertx.java.platform.Verticle;

public class Server extends Verticle {
  private static final String TEAM_INFO = "Arupaka, 0326-7345-1831\n";
  private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static String q1_output = TEAM_INFO + DATE_FORMATTER.format(new Date()) + "\n";
  private static SimpleDateFormat DEFORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static String q4 = "select txt from q4 where ttime=? limit 1;";

  public Server() {
    DEFORMATTER.setTimeZone(TimeZone.getTimeZone("GMT"));
    Thread t = new Thread(new Timer());
    t.setDaemon(true);
    t.start();
  } // Server()

  public void start() {
    vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {
      public void handle(HttpServerRequest req) {
        String path = req.path();
        MultiMap map = req.params();

        if ("/q1".equals(path)) {
          req.response().end(q1_output);
        } else if ("/q2".equals(path)) {
          String userid = map.get("userid");
          String ttime = map.get("tweet_time");
          
          StringBuilder q2_output = new StringBuilder(TEAM_INFO);
          try {
            ttime = String.valueOf(DEFORMATTER.parse(ttime).getTime());
            
            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;
            try {
              conn = DataSource.getInstance().getConnection();
              stmt = conn.createStatement();
            
              String sql = "select tweetid from q2 where userid="+userid+" and ttime="+ttime+";";
              rs = stmt.executeQuery(sql);
              while (rs.next()) {
                q2_output.append(rs.getString("tweetid"));
              }
              rs.close();
              stmt.close();
              conn.close();
            } catch (Exception e) {
            }

          } catch (Exception e) {
          } finally {
            req.response().end(q2_output.toString());
          }

         } else if ("/q3".equals(path)) {
          String userid = map.get("userid");
          
          StringBuilder q3_output = new StringBuilder(TEAM_INFO);
          Connection conn = null;
          Statement stmt = null;
          ResultSet rs = null;
          
          try {
            conn = DataSource.getInstance().getConnection();
            stmt = conn.createStatement();
            
            String sql = "select userid from q3 where orguserid="+userid+";";
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
              q3_output.append(rs.getString("userid"));
            }
            rs.close();
            stmt.close();
            conn.close();
          } catch (Exception e) {
          }
          req.response().end(q3_output.toString());

        } else if ("/q4".equals(path)) {
          String ttime = map.get("time");
          
          String q4_output = new String();
          try {
            ttime = String.valueOf(DEFORMATTER.parse(ttime).getTime());
            
            Connection conn = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
              conn = DataSource.getInstance().getConnection();
              stmt = conn.prepareStatement(q4);
              stmt.setString(1, ttime);

              rs = stmt.executeQuery();
              while (rs.next()) {
                q4_output = new String(rs.getBytes("txt"), "UTF-8");
              }
              rs.close();
              stmt.close();
              conn.close();
            } catch (Exception e) {
            }
          } catch (Exception e) {
          } finally {
            req.response().end(TEAM_INFO+q4_output);
          }

        } else if ("/q5".equals(path)) {
          String stime = map.get("start_time");
          String etime = map.get("end_time");
          String place = map.get("place");

          StringBuilder q5_output = new StringBuilder(TEAM_INFO);
          try {
            long stimeNum = DEFORMATTER.parse(stime).getTime();
            long etimeNum = DEFORMATTER.parse(etime).getTime();
            stime = String.valueOf(stimeNum);
            etime = String.valueOf(etimeNum);

            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;

            try {
              conn = DataSource.getInstance().getConnection();
              stmt = conn.createStatement();

              String sql = "select tweetid from q5 where place=\'"+place+"\' and time between "+stime+" and "+etime+";";
              rs = stmt.executeQuery(sql);
              while (rs.next()) {
                q5_output.append(rs.getString("tweetid")+"\n");
              }
              rs.close();
              stmt.close();
              conn.close();
            } catch (Exception e) {
            }
          } catch (Exception e) {
          } finally {
            req.response().end(q5_output.toString());
          }
        
        } else { // "/q6".queals(path)
          String idmin = map.get("userid_min");
          String idmax = map.get("userid_max");
          
          Connection conn = null;
          Statement stmt = null;
          ResultSet rs = null;
          int q6_output = 0;

          try {
            conn = DataSource.getInstance().getConnection();
            stmt = conn.createStatement();

            String sql = "select num, tot from q6 where userid="+idmin+" limit 1;";
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
              q6_output = rs.getInt("num") - rs.getInt("tot");
            }

            sql = "select tot from q6 where userid="+idmax+" limit 1;";
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
              q6_output += rs.getInt("tot");
            }
            rs.close();
            stmt.close();
            conn.close();
          } catch (Exception e) {
          }

          req.response().end(TEAM_INFO+String.valueOf(q6_output)+"\n");
        }
      }
    }).listen(8080);
  } // Server.start()

  class Timer implements Runnable {
    @Override
    public void run() {
      while (true) {
        try {
          long curr = new Date().getTime();
          q1_output = TEAM_INFO + DATE_FORMATTER.format(new Date()) + "\n";
          TimeUnit.MILLISECONDS.sleep(999);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } // Timer.run()
  } // Timer class
} // Server class

class DataSource {
  private static DataSource datasource;
  private ComboPooledDataSource cpds;
  private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
  private static final String JDBC_URL = "jdbc:mysql://localhost/cloud";
  private static final String USER = "root";
  private static final String PASSWD = "";
  private static final int MAXPOOLSIZE = 900;

  private DataSource() throws IOException, SQLException, PropertyVetoException{
    cpds = new ComboPooledDataSource();
    cpds.setDriverClass(JDBC_DRIVER);
    cpds.setJdbcUrl(JDBC_URL);
    cpds.setUser(USER);
    cpds.setPassword(PASSWD);

    cpds.setMaxPoolSize(MAXPOOLSIZE);
    cpds.setMaxStatements(1800);
    cpds.setNumHelperThreads(10);
  }

  public static DataSource getInstance() throws IOException, SQLException, PropertyVetoException {
    if (datasource == null) {
      datasource = new DataSource();
      return datasource;
    } else {
      return datasource;
    }
  }
  
  public Connection getConnection() throws SQLException {
    return this.cpds.getConnection();
  }
} // DataSource class
