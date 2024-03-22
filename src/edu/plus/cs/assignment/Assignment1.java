package edu.plus.cs.assignment;

import edu.plus.cs.util.DbUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Optional;

public class Assignment1 {

    public static void executeAssignment1() {
        String inputFile = "auth.tsv";

        // POSTGRES
        executeOnPostgres(inputFile);

        // MARIA DB
        executeOnMariaDb(inputFile);
    }

    private static void executeOnPostgres(String inputFile) {
        Optional<Connection> optionalConnection = DbUtils.connectToPostgres();

        if (optionalConnection.isEmpty()) {
            System.err.println("Could not establish connection to postgres");
            return;
        }

        Connection connection = optionalConnection.get();
        long startTimestamp;
        long endTimestamp;
        try {
            // create table
            String createTableSql = "CREATE TABLE IF NOT EXISTS public.auth (name character varying(49), pubid character varying(149))";
            Statement createTableStatement = connection.createStatement();

            createTableStatement.execute(createTableSql);
            createTableStatement.close();

            // insert method1
            startTimestamp = System.currentTimeMillis();
            insertMethod1(connection, inputFile);
            endTimestamp = System.currentTimeMillis();

            DbUtils.printTimestamps(startTimestamp, endTimestamp);
            DbUtils.printCount(connection, "public.auth");
            DbUtils.clearTable(connection, "public.auth");

            // insert method2
            startTimestamp = System.currentTimeMillis();
            insertMethod2(connection, inputFile);
            endTimestamp = System.currentTimeMillis();

            DbUtils.printTimestamps(startTimestamp, endTimestamp);
            DbUtils.printCount(connection, "public.auth");
            DbUtils.clearTable(connection, "public.auth");

            // insert method3
            startTimestamp = System.currentTimeMillis();
            insertMethod3(connection, inputFile);
            endTimestamp = System.currentTimeMillis();

            DbUtils.printTimestamps(startTimestamp, endTimestamp);
            DbUtils.printCount(connection, "public.auth");
            DbUtils.clearTable(connection, "public.auth");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void executeOnMariaDb(String inputFile) {
        Optional<Connection> optionalConnection = DbUtils.connectToMariaDb();

        if (optionalConnection.isEmpty()) {
            System.err.println("Could not establish connection to mariadb");
            return;
        }

        Connection connection = optionalConnection.get();
        long startTimestamp;
        long endTimestamp;
        try {
            // create table
            String createTableSql = "CREATE TABLE IF NOT EXISTS auth (name character varying(49), pubid character varying(149))";
            Statement createTableStatementMariaDb = connection.createStatement();

            createTableStatementMariaDb.execute(createTableSql);

            createTableStatementMariaDb.close();

            // insert method1 mariadb
            startTimestamp = System.currentTimeMillis();
            insertMethod1MariaDb(connection, inputFile);
            endTimestamp = System.currentTimeMillis();

            DbUtils.printTimestamps(startTimestamp, endTimestamp);
            DbUtils.printCount(connection, "auth");
            DbUtils.clearTable(connection, "auth");

            // insert method2 mariadb
            startTimestamp = System.currentTimeMillis();
            insertMethod2MariaDb(connection, inputFile);
            endTimestamp = System.currentTimeMillis();

            DbUtils.printTimestamps(startTimestamp, endTimestamp);
            DbUtils.printCount(connection, "auth");
            DbUtils.clearTable(connection, "auth");

            // insert method3 mariadb
            startTimestamp = System.currentTimeMillis();
            insertMethod3MariaDb(connection, inputFile);
            endTimestamp = System.currentTimeMillis();

            DbUtils.printTimestamps(startTimestamp, endTimestamp);
            DbUtils.printCount(connection, "auth");
            DbUtils.clearTable(connection, "auth");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void insertMethod1(Connection connection, String inputFile) {
        System.out.println("--- Insert method1 ---");
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            String line;
            PreparedStatement insertAuthorStatement = connection.prepareStatement("INSERT INTO public.auth (name, pubid) VALUES(?, ?)");
            while ((line = br.readLine()) != null) {
                String[] lineParts = line.split("\t");

                if (lineParts.length != 2) {
                    System.err.println("Read line not formatted correctly, got: " + line);
                    return;
                }

                insertAuthorStatement.setString(1, lineParts[0]);
                insertAuthorStatement.setString(2, lineParts[1]);

                insertAuthorStatement.execute();
            }
            insertAuthorStatement.close();
        } catch (IOException e) {
            System.err.println("Error reading from input file: " + e.getMessage());
            return;
        } catch (SQLException e) {
            System.err.println("Error while executing sql");
            throw new RuntimeException(e);
        }
    }

    private static void insertMethod2(Connection connection, String inputFile) {
        System.out.println("--- Insert method2 ---");
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            connection.setAutoCommit(false);
            String line;
            PreparedStatement insertAuthorStatement = connection.prepareStatement("INSERT INTO public.auth (name, pubid) VALUES(?, ?)");
            while ((line = br.readLine()) != null) {
                String[] lineParts = line.split("\t");

                if (lineParts.length != 2) {
                    System.err.println("Read line not formatted correctly, got: " + line);
                    return;
                }

                insertAuthorStatement.setString(1, lineParts[0]);
                insertAuthorStatement.setString(2, lineParts[1]);
                insertAuthorStatement.addBatch();
            }
            insertAuthorStatement.executeBatch();
            connection.commit();

            insertAuthorStatement.close();

            connection.setAutoCommit(true);
        } catch (IOException e) {
            System.err.println("Error reading from input file: " + e.getMessage());
            return;
        } catch (SQLException e) {
            System.err.println("Error while executing sql");
            throw new RuntimeException(e);
        }
    }

    private static void insertMethod3(Connection connection, String inputFile) {
        System.out.println("--- Insert method3 ---");
        String sqlCopy = "COPY public.auth (name, pubid) FROM '" + "/tmp/" + inputFile + "' DELIMITER E'\t'";
        try {
            Statement copyStatement = connection.createStatement();

            copyStatement.execute(sqlCopy);

            copyStatement.close();
        } catch (SQLException e) {
            System.err.println("Error while executing sql");
            throw new RuntimeException(e);
        }
    }

    private static void insertMethod1MariaDb(Connection connection, String inputFile) {
        System.out.println("--- Insert method1 MariaDB ---");
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            String line;
            PreparedStatement insertAuthorStatement = connection.prepareStatement("INSERT INTO auth (name, pubid) VALUES(?, ?)");
            while ((line = br.readLine()) != null) {
                String[] lineParts = line.split("\t");

                if (lineParts.length != 2) {
                    System.err.println("Read line not formatted correctly, got: " + line);
                    return;
                }

                insertAuthorStatement.setString(1, lineParts[0]);
                insertAuthorStatement.setString(2, lineParts[1]);

                insertAuthorStatement.execute();
            }
            insertAuthorStatement.close();
        } catch (IOException e) {
            System.err.println("Error reading from input file: " + e.getMessage());
            return;
        } catch (SQLException e) {
            System.err.println("Error while executing sql");
            throw new RuntimeException(e);
        }
    }

    private static void insertMethod2MariaDb(Connection connection, String inputFile) {
        System.out.println("--- Insert method2 ---");
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            connection.setAutoCommit(false);
            String line;
            PreparedStatement insertAuthorStatement = connection.prepareStatement("INSERT INTO auth (name, pubid) VALUES(?, ?)");
            while ((line = br.readLine()) != null) {
                String[] lineParts = line.split("\t");

                if (lineParts.length != 2) {
                    System.err.println("Read line not formatted correctly, got: " + line);
                    return;
                }

                insertAuthorStatement.setString(1, lineParts[0]);
                insertAuthorStatement.setString(2, lineParts[1]);
                insertAuthorStatement.addBatch();
            }
            insertAuthorStatement.executeBatch();
            connection.commit();

            insertAuthorStatement.close();

            connection.setAutoCommit(true);
        } catch (IOException e) {
            System.err.println("Error reading from input file: " + e.getMessage());
            return;
        } catch (SQLException e) {
            System.err.println("Error while executing sql");
            throw new RuntimeException(e);
        }
    }

    private static void insertMethod3MariaDb(Connection connection, String inputFile) {
        System.out.println("--- Insert method3 ---");
        String loadSql = "LOAD DATA INFILE '" + "/tmp/" + inputFile + "' INTO TABLE auth " +
                "FIELDS TERMINATED BY '\t' " +
                "LINES TERMINATED BY '\n'";

        try {
            Statement loadStatement = connection.createStatement();

            loadStatement.execute(loadSql);

            loadStatement.close();
        } catch (SQLException e) {
            System.err.println("Error while executing sql: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
