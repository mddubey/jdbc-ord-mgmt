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

    // Tests for Insert Statements
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

    @Test(expected = SQLIntegrityConstraintViolationException.class)
    public void InsertionFailsWhenPrimaryKeyIsAlreadyPresentInSameTable() throws SQLException {
        stmt = conn.createStatement();
        String insertOrder = "insert into order_info values(1,101,'2001-12-01','2001-12-05',1000);";
        String insertOrder2 = "insert into order_info values(1,101,'2001-12-01','2001-12-05',1000);";
        stmt.addBatch(insertOrder);
        stmt.addBatch(insertOrder2);

        stmt.executeBatch();
        stmt.close();
    }

    //Tests for select statements
    @Test
    public void testSelectsFieldsFromCustomerTable() throws Exception {
        stmt = conn.createStatement();
        String insertCustomer = "insert into customer values(101,'MRITUNJAY','C','JAVA','BIN',12345,'7689')";
        String selectCustomer = "SELECT cust_id,cust_name,contactNo from customer";

        stmt.executeUpdate(insertCustomer);
        ResultSet custoemrRS = stmt.executeQuery(selectCustomer);
        stmt.close();

        while (custoemrRS.next()) {
            Assert.assertEquals(101, custoemrRS.getInt("cust_id"));
            Assert.assertEquals("MRITUNJAY", custoemrRS.getString("cust_name"));
            Assert.assertEquals("7689", custoemrRS.getString("contactNo"));
        }
    }

    @Test
    public void testSelectsFieldsFromOrderTable() throws Exception {
        stmt = conn.createStatement();
        String insertCustomer = "insert into customer values(101,'MRITUNJAY','C:ProgramFiles','JAVA','BIN',12345,'7689')";
        String insertOrder = "insert into order_info values(1,101,'2001-12-01','2001-12-05',1000);";
        String selectOrder = "SELECT order_id, cust_id, date_of_order from order_info";

        stmt.executeUpdate(insertCustomer);
        stmt.executeUpdate(insertOrder);
        ResultSet orderRS = stmt.executeQuery(selectOrder);
        stmt.close();

        while (orderRS.next()) {
            Assert.assertEquals(1, orderRS.getInt(1));
            Assert.assertEquals(101, orderRS.getInt(2));
            Assert.assertEquals("2001-12-01", orderRS.getDate(3).toString());
        }
    }

    @Test
    public void testSelectsFieldsFromOrderTableWithConditions() throws Exception {
        stmt = conn.createStatement();
        String insertCustomer = "insert into customer values(101,'MRITUNJAY','C:ProgramFiles','JAVA','BIN',12345,'7689')";
        String insertOrder = "insert into order_info values(1,101,'2001-12-01','2001-12-05',1000);";
        String selectOrder = "SELECT order_id, cust_name, date_of_order from order_info,customer where order_info.cust_id = customer.cust_id";

        stmt.executeUpdate(insertCustomer);
        stmt.executeUpdate(insertOrder);
        ResultSet orderRS = stmt.executeQuery(selectOrder);
        stmt.close();

        while (orderRS.next()) {
            Assert.assertEquals(1, orderRS.getInt(1));
            Assert.assertEquals("MRITUNJAY", orderRS.getString(2));
            Assert.assertEquals("2001-12-01", orderRS.getDate(3).toString());
        }
    }

    //    tests for update statements

    @Test
    public void testUpdatesDetailsOfOneCustomer() throws Exception {
        stmt = conn.createStatement();
        String insertCustomerMritunjay = "insert into customer values(101,'MRITUNJAY','C','JAVA','BIN',12345,'7689');";
        String insertCustomerShubham = "insert into customer values (102,'SHUBHAM','c','JAVA','BIN',123,'0551-987');";
        stmt.addBatch(insertCustomerMritunjay);
        stmt.addBatch(insertCustomerShubham);
        String updateCustomer = "update customer set cust_name = 'DUBEY' where cust_id = '101'";
        String selectCustomer = "select cust_name from customer";
        int expectedAffectedRows = 1;

        stmt.executeBatch();
        int actualAffectedRows = stmt.executeUpdate(updateCustomer);
        ResultSet customerRS = stmt.executeQuery(selectCustomer);
        stmt.close();

        Assert.assertEquals(expectedAffectedRows, actualAffectedRows);

        customerRS.next();
        Assert.assertEquals("DUBEY", customerRS.getString(1));
        customerRS.next();
        Assert.assertEquals("SHUBHAM", customerRS.getString(1));
    }

    @Test
    public void testUpdatesDetailsOfCustomer() throws Exception {
        stmt = conn.createStatement();
        String insertCustomerMritunjay = "insert into customer values(101,'MRITUNJAY','C','JAVA','BIN',12345,'7689');";
        String insertCustomerShubham = "insert into customer values (102,'SHUBHAM','c','JAVA','BIN',123,'0551-987');";
        stmt.addBatch(insertCustomerMritunjay);
        stmt.addBatch(insertCustomerShubham);
        String updateCustomer = "update customer set cust_name = 'DUBEY'";
        String selectCustomer = "select cust_name from customer";
        int expectedAffectedRows = 2;

        stmt.executeBatch();
        int actualAffectedRows = stmt.executeUpdate(updateCustomer);
        ResultSet customerRS = stmt.executeQuery(selectCustomer);
        stmt.close();

        Assert.assertEquals(expectedAffectedRows, actualAffectedRows);

        while (customerRS.next()) {
            Assert.assertEquals("DUBEY", customerRS.getString(1));
        }
    }

//    Tests of Delete statements

    @Test
    public void testDeletesSingleCustomer() throws Exception {
        stmt = conn.createStatement();
        String insertCustomer = "insert into customer values(101,'MRITUNJAY','C','JAVA','BIN',12345,'7689');";
        String deleteCustomer = "delete from customer where cust_id = 101";

        stmt.executeUpdate(insertCustomer);
        int affectedRows = stmt.executeUpdate(deleteCustomer);
        stmt.close();

        Assert.assertEquals(1, affectedRows);
    }

    @Test
    public void testDeletesAllCustomers() throws Exception {
        stmt = conn.createStatement();
        String insertCustomerMritunjay = "insert into customer values(101,'MRITUNJAY','C','JAVA','BIN',12345,'7689');";
        String insertCustomerShubham = "insert into customer values (102,'SHUBHAM','c','JAVA','BIN',123,'0551-987');";
        stmt.addBatch(insertCustomerMritunjay);
        stmt.addBatch(insertCustomerShubham);
        String deleteAllCustomesr = "delete from customer";

        stmt.executeBatch();
        int affectedRows = stmt.executeUpdate(deleteAllCustomesr);
        stmt.close();

        Assert.assertEquals(2, affectedRows);
    }


    @Test
    public void testDeletesCustomerAndOrder() throws Exception {
        stmt = conn.createStatement();
        String insertCustomer = "insert into customer values(101,'MRITUNJAY','C:ProgramFiles','JAVA','BIN',12345,'7689')";
        String insertOrder = "insert into order_info values(1,101,'2001-12-01','2001-12-05',1000);";
        stmt.addBatch(insertCustomer);
        stmt.addBatch(insertOrder);
        String deleteOrder = "delete from order_info where order_id = 1";
        String deleteCustomer = "delete from customer where cust_id = 101";

        stmt.executeBatch();
        int affectedRowsInOrder = stmt.executeUpdate(deleteOrder);
        int affectedRowsInCustomer = stmt.executeUpdate(deleteCustomer);
        stmt.close();

        Assert.assertEquals(1, affectedRowsInCustomer);
        Assert.assertEquals(1, affectedRowsInOrder);

    }

    @Test(expected = SQLIntegrityConstraintViolationException.class)
    public void testDeletionFailsWhenCustomerIdIsBeingUsedAsForeignKey() throws Exception {
        stmt = conn.createStatement();
        String insertCustomer = "insert into customer values(101,'MRITUNJAY','C:ProgramFiles','JAVA','BIN',12345,'7689')";
        String insertOrder = "insert into order_info values(1,101,'2001-12-01','2001-12-05',1000);";
        stmt.addBatch(insertCustomer);
        stmt.addBatch(insertOrder);
        String deleteCustomer = "delete from customer where cust_id = 101";

        stmt.executeBatch();
        stmt.executeUpdate(deleteCustomer);
        stmt.close();
    }
}
