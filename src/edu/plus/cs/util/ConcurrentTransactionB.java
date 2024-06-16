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

public class ConcurrentTransactionB extends Thread {

	// identifier of the transaction
	int id;

	Connection connection;

	public ConcurrentTransactionB(int id, Connection connection) {
		this.id = id;
		this.connection = connection;
	}

	@Override
	public void run() {
		/*
			UPDATE Accounts SET balance=balance+1 WHERE account=i
			UPDATE Accounts SET balance=balance-1 WHERE account=0
		 */

		ResultSet resultSet;
		try {
			PreparedStatement setBalanceStatement = connection.prepareStatement("UPDATE public.accounts SET balance = balance + ? WHERE account = ?");
			int value;

			// handle account i
			setBalanceStatement.setInt(1, 1);
			setBalanceStatement.setInt(2, id);
			setBalanceStatement.execute();

			// handle account 0
			setBalanceStatement.setInt(1, -1);
			setBalanceStatement.setInt(2, 0);
			setBalanceStatement.execute();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}