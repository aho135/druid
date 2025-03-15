import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DruidJDBCQuery
{
  private static final String DRUID_JDBC_URL = "jdbc:avatica:remote:url=http://localhost:8888/druid/v2/sql/avatica/";
  private static final String QUERY = "SELECT * FROM events_sink ORDER BY __time DESC LIMIT 1";
  private static final int CONCURRENT_QUERIES = 3;
  public static void main(String[] args) throws Exception
  {
    Class.forName("org.apache.calcite.avatica.remote.Driver");
    ExecutorService executorService = Executors.newFixedThreadPool(CONCURRENT_QUERIES);
    List<Future<String>> queryFutures = new ArrayList<>();
    for (int count = 0; count < CONCURRENT_QUERIES; count++) {
      Future<String> future = executorService.submit(() -> executeQuery(QUERY));
      queryFutures.add(future);
    }
    for (Future<String> future : queryFutures) {
      System.out.println(future.get());
    }
    executorService.shutdown();
  }

  private static String executeQuery(String query) throws SQLException
  {
    try (
        Connection connection = DriverManager.getConnection(DRUID_JDBC_URL);
        Statement statement = connection.createStatement();
        final ResultSet rs = statement.executeQuery(query);
    ) {
      StringBuilder builder = new StringBuilder();
      while (rs.next()) {
        for (int ordinal = 1; ordinal <= rs.getMetaData().getColumnCount(); ordinal++) {
          String columnName = rs.getMetaData().getColumnName(ordinal);
          String columnValue = "";
          if (rs.getObject(ordinal) != null) {
            columnValue = rs.getObject(ordinal).toString();
          }
          builder.append(columnName);
          builder.append("=");
          builder.append(columnValue);
          builder.append(";");
        }
      }
      return builder.toString();
    }
  }
}
