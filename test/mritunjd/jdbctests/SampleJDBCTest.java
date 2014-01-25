package mritunjd.jdbctests;

import org.junit.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SampleJDBCTest {
    static final String DB_URL = "jdbc:mysql://localhost:3306/jdbc-test";

    static final String USER = "mritunjay";
    static final String PASS = "12345";

    private static Connection conn;
    private Statement stmt;

    @BeforeClass
    public static void createConnection() throws ClassNotFoundException, SQLException {
        conn = DriverManager.getConnection(DB_URL, USER, PASS);
    }

    @Before
    public void setUp() throws SQLException {
        stmt = conn.createStatement();
        String sql = "create table product_info(\n" +
                "product_id int,\n" +
                "product_name varchar(25),\n" +
                "unit_price float,\n" +
                "catagory varchar(10)\n" +
                ")";

        stmt.execute(sql);
        stmt.close();
    }

    @After
    public void tearDown() throws SQLException {
        stmt = conn.createStatement();
        String sql = "drop table product_info";
        stmt.execute(sql);
        stmt.close();
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        conn.close();
    }
    @Test
    public void test(){

    }
}
