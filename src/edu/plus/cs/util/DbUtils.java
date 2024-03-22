package edu.plus.cs.util;

import java.sql.*;
import java.util.Optional;

public class DbUtils {
    public static void clearTable(Connection connection, String table) {
        try {
            Statement clearTableStatement = connection.createStatement();
            String clearTableSql = "DELETE FROM " + table;
            clearTableStatement.execute(clearTableSql);
            clearTableStatement.close();
        } catch (SQLException e) {
            System.err.println("Could not clear table: " + table);
            throw new RuntimeException(e);
        }
    }

    public static void printTimestamps(long startTimestamp, long endTimestamp) {
        System.out.println("Started at: " + startTimestamp);
        System.out.println("Finished at: " + endTimestamp);
        System.out.println("Time needed: " + (endTimestamp - startTimestamp));
    }

    public static Optional<Connection> connectToPostgres() {
        String url = "jdbc:postgresql://localhost/db-tuning-1?user=postgres&password=admin";
        try {
            return Optional.of(DriverManager.getConnection(url));
        } catch (SQLException e) {
            System.err.println("Could not establish db connection: " + e.getMessage());
            return Optional.empty();
        }
    }

    public static Optional<Connection> connectToMariaDb() {
        String url = "jdbc:mariadb://localhost/db_tuning_1?user=root&password=admin";
        try {
            return Optional.of(DriverManager.getConnection(url));
        } catch (SQLException e) {
            System.err.println("Could not establish db connection: " + e.getMessage());
            return Optional.empty();
        }
    }

    public static void printCount(Connection connection, String table) {
        try {
            Statement countStatement = connection.createStatement();
            String countStatementSql = "SELECT COUNT(*) AS rowcount FROM " + table;

            ResultSet resultSet = countStatement.executeQuery(countStatementSql);

            if (resultSet.next()) {
                int count = resultSet.getInt("rowcount");
                System.out.println("Number of tuples: " + count);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
