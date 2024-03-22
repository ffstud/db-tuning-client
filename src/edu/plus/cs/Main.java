package edu.plus.cs;

import edu.plus.cs.assignment.Assignment1;
import edu.plus.cs.util.DbUtils;

import java.sql.*;
import java.util.Optional;

public class Main {
    public static void main(String[] args) {
        Optional<Connection> connection = DbUtils.connectToMariaDb();

        if (connection.isPresent()) {
            System.out.println("------- Assignment 1 -------");
            Assignment1.executeAssignment1(connection.get());
        } else {
            System.err.println("Could not execute assignment 1: no database connection present!");
        }
    }


}
