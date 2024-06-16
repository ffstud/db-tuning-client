package edu.plus.cs.assignment;

import edu.plus.cs.util.ConcurrentTransactionA;
import edu.plus.cs.util.ConcurrentTransactionB;
import edu.plus.cs.util.DbUtils;

import java.sql.*;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Assignment6 {
    public static void executeAssignment6(String transactionType, String isolationLevel) {
        Optional<Connection> optionalConnection = DbUtils.connectToPostgres("db-tuning-1");

        if (optionalConnection.isEmpty()) {
            System.err.println("Could not establish connection to postgres");
            return;
        }

        Connection connection = optionalConnection.get();

        try {
            // execute transactions
            executeTransactions(connection, transactionType, isolationLevel);

            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void executeTransactions(Connection connection, String transactionType, String isolationLevel) throws SQLException {
        // set isolationLevel accordingly
        Statement setIsolationLevelStatement = connection.createStatement();
        if ("COMMITTED".equals(isolationLevel)) {
            setIsolationLevelStatement = connection.createStatement();
            setIsolationLevelStatement.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
        } else if ("SERIALIZABLE".equals(isolationLevel)) {
            setIsolationLevelStatement = connection.createStatement();
            setIsolationLevelStatement.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;");
        } else {
            System.err.println("Invalid isolationLevel provided!");
            return;
        }

        System.out.println("------ Isolation level " + isolationLevel + " ------");

        long startTime = 0;
        long endTime = 0;
        ResultSet resultSet;
        for (int numberOfThreads = 1; numberOfThreads <= 5; numberOfThreads++) {
            // here we need to reset and prepare the data before each run
            // create accounts table
            createAccountsTable(connection);

            // populate accounts table
            populateAccountsTable(connection);

            Thread[] trans = null;
            if ("A".equals(transactionType)) {
                // prepare transactions
                trans = new ConcurrentTransactionA[100];
                for (int i = 0; i < trans.length; i++) {
                    trans[i] = new ConcurrentTransactionA(i + 1, connection);
                }
            } else if ("B".equals(transactionType)) {
                // prepare transactions
                trans = new ConcurrentTransactionB[100];
                for (int i = 0; i < trans.length; i++) {
                    trans[i] = new ConcurrentTransactionB(i + 1, connection);
                }
            } else {
                System.err.println("Invalid transaction type provided!");
                return;
            }

            System.out.println("------ Approach " + transactionType + " ------");

            // start all transactions using a thread pool
            startTime = System.currentTimeMillis();
            ExecutorService pool = Executors.newFixedThreadPool(numberOfThreads);
            for (int i = 0; i < trans.length; i++) {
                pool.execute(trans[i]);
            }

            pool.shutdown(); // end program after all transactions are done
            while (!pool.isTerminated());
            endTime = System.currentTimeMillis();

            // calculate values
            Statement readAccount0Balance = connection.createStatement();
            resultSet = readAccount0Balance.executeQuery("SELECT balance from public.accounts WHERE account = 0");
            resultSet.next();

            int balance0 = resultSet.getInt("balance");
            double correctness = (100.0 - balance0) / 100.0;

            System.out.println("---- " + numberOfThreads + " concurrent thread(s) ----");
            System.out.println("Correctness: " + correctness);
            System.out.println("Time needed (throughput): " + (endTime - startTime));
        }
    }

    private static void createAccountsTable(Connection connection) throws SQLException {
        DbUtils.removeTableIfExists(connection, "accounts");

        String createTableSql = "CREATE TABLE IF NOT EXISTS public.accounts (account INT, balance INT)";
        Statement createTableStatement = connection.createStatement();

        createTableStatement.execute(createTableSql);
        createTableStatement.close();
    }

    private static void populateAccountsTable(Connection connection) throws SQLException {
        PreparedStatement insertAccountsStatement = connection.prepareStatement("INSERT INTO public.accounts (account, balance) VALUES(?, ?)");
        connection.setAutoCommit(false);

        // add account 0
        insertAccountsStatement.setInt(1, 0);
        insertAccountsStatement.setInt(2, 100);

        insertAccountsStatement.addBatch();

        // add accounts 1 - 100
        for (int i = 1; i <= 100; i++) {
            insertAccountsStatement.setInt(1, i);
            insertAccountsStatement.setInt(2, 0);

            insertAccountsStatement.addBatch();
        }

        insertAccountsStatement.executeBatch();
        connection.commit();

        insertAccountsStatement.close();

        connection.setAutoCommit(true);
    }
}
