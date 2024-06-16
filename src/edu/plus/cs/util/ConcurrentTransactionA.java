/*
 * Example code for Assignment 6 (concurrency tuning) of the course:
 * 
 * Database Tuning
 * Department of Computer Science
 * University of Salzburg, Austria
 * 
 * Lecturer: Nikolaus Augsten
 */
package edu.plus.cs.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConcurrentTransactionA extends Thread {

	// identifier of the transaction
	int id;

	Connection connection;

	public ConcurrentTransactionA(int id, Connection connection) {
		this.id = id;
		this.connection = connection;
	}
	
	@Override
	public void run() {
		/*
			e ← SELECT balance FROM Accounts WHERE account=i
			UPDATE Accounts SET balance=e + 1 WHERE account=i
			c ← SELECT balance FROM Accounts WHERE account=0
			UPDATE Accounts SET balance=c − 1 WHERE account=0
		 */
		ResultSet resultSet;
		try {
			PreparedStatement readCurrentBalanceStatement = connection.prepareStatement("SELECT balance from public.accounts WHERE account = ?");
			PreparedStatement setBalanceStatement = connection.prepareStatement("UPDATE public.accounts SET balance = ? WHERE account = ?");
			int value;

			// handle account i
			readCurrentBalanceStatement.setInt(1, id);
			resultSet = readCurrentBalanceStatement.executeQuery();
			resultSet.next();
			value = resultSet.getInt("balance");

			value = value + 1;

			setBalanceStatement.setInt(1, value);
			setBalanceStatement.setInt(2, id);
			setBalanceStatement.execute();

			// handle account 0
			readCurrentBalanceStatement.setInt(1, 0);
			resultSet = readCurrentBalanceStatement.executeQuery();
			resultSet.next();
			value = resultSet.getInt("balance");

			value = value - 1;

			setBalanceStatement.setInt(1, value);
			setBalanceStatement.setInt(2, 0);
			setBalanceStatement.execute();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
}
