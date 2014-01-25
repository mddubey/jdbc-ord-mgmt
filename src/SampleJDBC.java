import java.sql.*;

public class SampleJDBC {
    static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost/test";

    static final String USER = "mritunjay";
    static final String PASS = "12345";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName(JDBC_DRIVER);

            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            System.out.println("Creating statement...");
            stmt = conn.createStatement();
            String sql;
            sql = "SELECT rollNo,name from student";
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                int rollNo = rs.getInt("rollNo");
                String name = rs.getString("name");

                System.out.println("Roll No: " + rollNo);
                System.out.println("Name: " + name);
            }
            rs.close();
            stmt.close();
            conn.close();

        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
                System.err.println("nothing we can do");
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
        System.out.println("Goodbye!");
    }
}
