package edu.plus.cs.assignment;

import edu.plus.cs.util.DbUtils;

import java.sql.*;
import java.util.*;

public class Assignment2 {

    public static void executeAssignment2() {
        try {
            // POSTGRES
            System.out.println("--- Executing queries on Postgres:");
            Optional<Connection> optionalOfConnection = DbUtils.connectToPostgres("db-tuning-2");

            if (optionalOfConnection.isEmpty()) {
                System.err.println("Could not establish connection to postgres");
                return;
            }

            Connection connection = optionalOfConnection.get();

            // prologue
            DbUtils.removeTableIfExists(connection, "Employee");
            DbUtils.removeTableIfExists(connection, "Student");
            DbUtils.removeTableIfExists(connection, "Techdept");

            createTables(connection);
            createIndexes(connection);
            populateTables(connection);

            // query 1
            List<Integer> ssNumsAverageOriginalQuery = executeQuery1(connection);
            List<Integer> ssNumsAverageRewrittenQuery = executeRewrittenQuery1(connection);
            if (!(ssNumsAverageOriginalQuery.containsAll(ssNumsAverageRewrittenQuery)
                    && ssNumsAverageRewrittenQuery.containsAll(ssNumsAverageOriginalQuery))) {
                System.err.println("Result of original and rewritten average query does not match!");
                return;
            }

            // query 2
            List<Integer> ssNumsTechdeptOriginalQuery = executeQuery2(connection);
            List<Integer> ssNumsTechdeptRewrittenQuery = executeRewrittenQuery2(connection);
            if (!(ssNumsTechdeptOriginalQuery.containsAll(ssNumsTechdeptRewrittenQuery)
                    && ssNumsTechdeptRewrittenQuery.containsAll(ssNumsTechdeptOriginalQuery))) {
                System.err.println("Result of original and rewritten techdept query does not match!");
                return;
            }

            connection.close();

            System.out.println("--- Executing queries on Maria DB:");
            // MARIADB
            optionalOfConnection = DbUtils.connectToMariaDb("db_tuning_2");

            if (optionalOfConnection.isEmpty()) {
                System.err.println("Could not establish connection to maria db");
                return;
            }

            connection = optionalOfConnection.get();

            DbUtils.removeTableIfExists(connection, "Employee");
            DbUtils.removeTableIfExists(connection, "Student");
            DbUtils.removeTableIfExists(connection, "Techdept");

            createTables(connection);
            createIndexes(connection);
            populateTables(connection);

            // query 1
            ssNumsAverageOriginalQuery = executeQuery1(connection);
            ssNumsAverageRewrittenQuery = executeRewrittenQuery1(connection);
            if (!(ssNumsAverageOriginalQuery.containsAll(ssNumsAverageRewrittenQuery) && ssNumsAverageRewrittenQuery.containsAll(ssNumsAverageOriginalQuery))) {
                System.err.println("Result of original and rewritten query does not match!");
                return;
            }

            // query 2
            ssNumsTechdeptOriginalQuery = executeQuery2(connection);
            ssNumsTechdeptRewrittenQuery = executeRewrittenQuery2(connection);
            if (!(ssNumsTechdeptOriginalQuery.containsAll(ssNumsTechdeptRewrittenQuery)
                    && ssNumsTechdeptRewrittenQuery.containsAll(ssNumsTechdeptOriginalQuery))) {
                System.err.println("Result of original and rewritten techdept query does not match!");
                return;
            }

            connection.close();
        } catch (SQLException e) {
            System.err.println("Error while executing assignment 2");
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    private static List<Integer> executeQuery1(Connection connection) throws SQLException {
        long start, end;
        List<Integer> aboveAverageEmployees = new ArrayList<>();

        StringBuilder stringBuilder = new StringBuilder();
        Statement query1Statement = connection.createStatement();

        stringBuilder.append("SELECT ssnum FROM Employee e1 WHERE salary > (");
        stringBuilder.append("SELECT AVG(e2.salary) FROM Employee e2, Techdept WHERE e2.dept = e1.dept AND e2.dept = Techdept.dept)");

        System.out.println("Executing original query 1:");
        start = System.currentTimeMillis();
        ResultSet query1ResultSet = query1Statement.executeQuery(stringBuilder.toString());
        end = System.currentTimeMillis();

        DbUtils.printTimestamps(start, end);

        while (query1ResultSet.next()) {
            // System.out.print(query1ResultSet.getInt("ssnum") + ", ");
            aboveAverageEmployees.add(query1ResultSet.getInt("ssnum"));
        }

        return aboveAverageEmployees;
    }

    private static List<Integer> executeRewrittenQuery1(Connection connection) throws SQLException {
        long start, end;
        List<Integer> aboveAverageEmployees = new ArrayList<>();

        DbUtils.removeTableIfExists(connection, "Temp");

        StringBuilder stringBuilder = new StringBuilder();
        Statement rewrittenQuery1Statement = connection.createStatement();

        stringBuilder.append("CREATE TEMPORARY TABLE Temp AS (");
        stringBuilder.append("SELECT AVG(salary) as avsalary, e2.dept FROM Employee e2, Techdept ");
        stringBuilder.append("WHERE e2.dept = Techdept.dept ");
        stringBuilder.append("GROUP BY e2.dept)");

        System.out.println("Executing rewritten query 1:");
        start = System.currentTimeMillis();
        rewrittenQuery1Statement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        stringBuilder.append("SELECT ssnum FROM Employee e1, Temp WHERE salary > avsalary AND e1.dept = Temp.dept");

        ResultSet rewrittenQuery1ResultSet = rewrittenQuery1Statement.executeQuery(stringBuilder.toString());
        end = System.currentTimeMillis();

        DbUtils.printTimestamps(start, end);

        while (rewrittenQuery1ResultSet.next()) {
            // System.out.print(rewrittenQuery1ResultSet.getInt("ssnum") + ", ");
            aboveAverageEmployees.add(rewrittenQuery1ResultSet.getInt("ssnum"));
        }

        return aboveAverageEmployees;
    }

    private static List<Integer> executeQuery2(Connection connection) throws SQLException {
        long start, end;
        List<Integer> techDeptEmployees = new ArrayList<>();

        StringBuilder stringBuilder = new StringBuilder();
        Statement query2Statement = connection.createStatement();

        stringBuilder.append("SELECT ssnum FROM Employee WHERE dept IN (SELECT dept FROM Techdept)");

        System.out.println("Executing original query 2:");
        start = System.currentTimeMillis();
        ResultSet query2ResultSet = query2Statement.executeQuery(stringBuilder.toString());
        end = System.currentTimeMillis();

        DbUtils.printTimestamps(start, end);

        while (query2ResultSet.next()) {
            // System.out.print(query2ResultSet.getInt("ssnum") + ", ");
            techDeptEmployees.add(query2ResultSet.getInt("ssnum"));
        }

        return techDeptEmployees;
    }

    private static List<Integer> executeRewrittenQuery2(Connection connection) throws SQLException {
        long start, end;
        List<Integer> techDeptEmployees = new ArrayList<>();

        StringBuilder stringBuilder = new StringBuilder();
        Statement rewrittenQuery2Statement = connection.createStatement();

        stringBuilder.append("SELECT ssnum FROM Employee, Techdept WHERE Employee.dept = Techdept.dept");

        System.out.println("Executing rewritten query 2:");
        start = System.currentTimeMillis();
        ResultSet rewrittenQuery2ResultSet = rewrittenQuery2Statement.executeQuery(stringBuilder.toString());
        end = System.currentTimeMillis();

        DbUtils.printTimestamps(start, end);

        while (rewrittenQuery2ResultSet.next()) {
            // System.out.print(rewrittenQuery2ResultSet.getInt("ssnum") + ", ");
            techDeptEmployees.add(rewrittenQuery2ResultSet.getInt("ssnum"));
        }

        return techDeptEmployees;
    }

    private static void populateTables(Connection connection) throws SQLException {
        Random random = new Random();
        // populate Techdept table
        // 10 entries
        connection.setAutoCommit(false);
        PreparedStatement insertBatchStatement = connection.prepareStatement("INSERT INTO Techdept (dept, manager, location) VALUES(?, ?, ?)");

        HashMap<String, String> techDeptManagerRelation = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            String department = "Department_" + (i + 1);
            String manager = "Name_" + (random.nextInt(1, 11));
            techDeptManagerRelation.put(department, manager);
            insertBatchStatement.setString(1, department);
            insertBatchStatement.setString(2, manager);
            insertBatchStatement.setString(3, "Location_" + (random.nextInt(1, 11)));

            insertBatchStatement.addBatch();
        }

        insertBatchStatement.executeBatch();
        connection.commit();

        insertBatchStatement.close();

        // populate Employee table
        // 100k entries, only 10% of the employees should belong to a techdept
        insertBatchStatement = connection.prepareStatement("INSERT INTO Employee (ssnum, name, manager, dept, salary, numfriends) VALUES (?, ?, ?, ?, ?, ?)");

        for (int i = 0; i < 100000; i++) {
            insertBatchStatement.setInt(1, (i + 1));
            insertBatchStatement.setString(2, "Name_" + (i + 1));

            // only 10% of the employees should belong the tech departments
            if (random.nextDouble() <= 0.1) {
                int departmentNumber = random.nextInt(1, 11);
                String manager = techDeptManagerRelation.get("Department_" + departmentNumber);

                insertBatchStatement.setString(3, manager);
                insertBatchStatement.setString(4, "Department_" + departmentNumber);
            } else {
                insertBatchStatement.setString(3, "Name_" + (random.nextInt(11, 1001)));
                insertBatchStatement.setString(4, "Department_" + (random.nextInt(11, 101)));
            }

            insertBatchStatement.setInt(5, 10000 * random.nextInt(1, 16));
            insertBatchStatement.setInt(6, random.nextInt(0, 11));

            insertBatchStatement.addBatch();
        }

        insertBatchStatement.executeBatch();
        connection.commit();

        insertBatchStatement.close();

        // populate Student table
        insertBatchStatement = connection.prepareStatement("INSERT INTO Student (ssnum, name, course, grade) VALUES (?, ?, ?, ?)");

        for (int i = 0; i < 100000; i++) {
            insertBatchStatement.setInt(1, 80000 + i);
            insertBatchStatement.setString(2, "Name_" + (80000 + i));
            insertBatchStatement.setString(3, "Course_" + random.nextInt(1, 10000));
            insertBatchStatement.setInt(4, random.nextInt(1, 6));

            insertBatchStatement.addBatch();
        }

        insertBatchStatement.executeBatch();
        connection.commit();

        insertBatchStatement.close();

        connection.setAutoCommit(true);
    }

    private static void createTables(Connection connection) throws SQLException {
        // create Employee, Student and Techdept tables

        // EMPLOYEE
        // create table
        String tableName = "Employee";
        StringBuilder stringBuilder = new StringBuilder();
        Statement createTableStatement = connection.createStatement();

        // remove table if exists
        DbUtils.removeTableIfExists(connection, tableName);

        stringBuilder.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
        stringBuilder.append("ssnum INTEGER PRIMARY KEY, ");
        stringBuilder.append("name VARCHAR(64) UNIQUE NOT NULL, ");
        stringBuilder.append("manager VARCHAR(64), ");
        stringBuilder.append("dept VARCHAR(64), ");
        stringBuilder.append("salary INTEGER, ");
        stringBuilder.append("numfriends INTEGER)");

        createTableStatement.execute(stringBuilder.toString());

        // STUDENT
        tableName = "Student";
        stringBuilder.setLength(0);
        createTableStatement = connection.createStatement();

        // remove table if exists
        DbUtils.removeTableIfExists(connection, tableName);

        stringBuilder.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
        stringBuilder.append("ssnum INTEGER PRIMARY KEY, ");
        stringBuilder.append("name VARCHAR(64) UNIQUE NOT NULL, ");
        stringBuilder.append("course VARCHAR(64), ");
        stringBuilder.append("grade INTEGER)");

        createTableStatement.execute(stringBuilder.toString());

        // TECHDEPT
        tableName = "Techdept";
        stringBuilder.setLength(0);
        createTableStatement = connection.createStatement();

        // remove table if exists
        DbUtils.removeTableIfExists(connection, tableName);

        stringBuilder.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
        stringBuilder.append("dept VARCHAR(64) PRIMARY KEY, ");
        stringBuilder.append("manager VARCHAR(64), ");
        stringBuilder.append("location VARCHAR(64))");

        createTableStatement.execute(stringBuilder.toString());
    }

    private static void createIndexes(Connection connection) {
        DbUtils.createIndexOnAttribute("Employee", "idx_ssnum", "ssnum", true, connection);
        DbUtils.createIndexOnAttribute("Employee", "idx_name", "name", true, connection);
        DbUtils.createIndexOnAttribute("Employee", "idx_dept", "dept", false, connection);

        DbUtils.createIndexOnAttribute("Student", "idx_ssnum", "ssnum", true, connection);
        DbUtils.createIndexOnAttribute("Student", "idx_name", "name", true, connection);

        DbUtils.createIndexOnAttribute("Techdept", "idx_dept", "dept", true, connection);
    }
}
