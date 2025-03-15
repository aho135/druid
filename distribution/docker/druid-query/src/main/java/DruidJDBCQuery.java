import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class DruidJDBCQuery
{
  private static final String DRUID_JDBC_URL = "jdbc:avatica:remote:url=http://localhost:8888/druid/v2/sql/avatica/";
  private static final String QUERY = "SELECT * FROM events_sink ORDER BY __time DESC LIMIT 1";
  public static void main(String[] args) throws Exception
  {
    Class.forName("org.apache.calcite.avatica.remote.Driver");
    try (
        Connection connection = DriverManager.getConnection(DRUID_JDBC_URL);
        Statement statement = connection.createStatement();
        final ResultSet rs = statement.executeQuery(QUERY);
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
        System.out.println(builder);
    }
  }
}
