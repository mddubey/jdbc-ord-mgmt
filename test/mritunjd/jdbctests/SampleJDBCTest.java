package mritunjd.jdbctests;

import org.junit.*;

import java.sql.*;


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
    public void createTablesAndRelations() throws SQLException {
        // Create customers TABLE
        stmt = conn.createStatement();
        String sql = "create table customer(\n" +
                "cust_id int,\n" +
                "cust_name varchar(30),\n" +
                "add1 varchar(15),\n" +
                "add2 varchar(15),\n" +
                "city varchar(15),\n" +
                "pinNo int,\n" +
                "contactNo varchar(13)\n" +
                ");";

        stmt.execute(sql);
        stmt.close();

//        Cretate primary key cust_id
        stmt = conn.createStatement();
        sql = "\n" +
                "alter table customer add constraint primary key(cust_id);";
        stmt.execute(sql);
        stmt.close();

        // Create order TABLE
        stmt = conn.createStatement();
        sql = "create table order_info(\n" +
                "order_id int,\n" +
                "cust_id int,\n" +
                "date_of_order date,\n" +
                "date_of_delivery date,\n" +
                "total_bill float\n" +
                ");";

        stmt.execute(sql);
        stmt.close();

//        Cretate primary key order_id
        stmt = conn.createStatement();
        sql = "\n" +
                "alter table order_info add constraint primary key(order_id);";
        stmt.execute(sql);
        stmt.close();


        //        Create foreign key cust_id
        stmt = conn.createStatement();
        sql = "alter table order_info add constraint order_fk_cust_id foreign key(cust_id)\n" +
                "                references customer(cust_id);";
        stmt.execute(sql);
        stmt.close();


    }

    @After
    public void tearDown() throws SQLException {
        stmt = conn.createStatement();
        String sql = "drop table order_info";
        stmt.execute(sql);
        stmt.close();

        stmt = conn.createStatement();
        sql = "drop table customer";
        stmt.execute(sql);
        stmt.close();

    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        conn.close();
    }

    @Test
    public void testInsertsOneRowInEachTable() throws SQLException {
        stmt = conn.createStatement();
        String insertCustomer = "insert into customer values(101,'MRITUNJAY','C:ProgramFiles','JAVA','BIN',12345,'7689')";
        String insertOrder = "insert into order_info values(1,101,'2001-12-01','2001-12-05',1000);";

        int affectedRowsInCustomer = stmt.executeUpdate(insertCustomer);
        int affectedRowsInOrder = stmt.executeUpdate(insertOrder);
        stmt.close();

        Assert.assertEquals(1, affectedRowsInCustomer);
        Assert.assertEquals(1, affectedRowsInOrder);
    }

    @Test(expected = SQLIntegrityConstraintViolationException.class)
    public void InsertionFailsWhenCustomerIsNotPresentGivenInOrderSql() throws SQLException {
        stmt = conn.createStatement();
        String insertOrder = "insert into order_info values(1,101,'2001-12-01','2001-12-05',1000);";

        stmt.executeUpdate(insertOrder);
        stmt.close();
    }
}
