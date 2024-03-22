package edu.plus.cs;

import edu.plus.cs.assignment.Assignment1;
import edu.plus.cs.util.AssignmentType;
import edu.plus.cs.util.DbUtils;

import java.sql.*;
import java.util.Optional;

public class Main {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Invalid call, no assignment given");
        }

        AssignmentType assignmentType = null;
        try {
            assignmentType = AssignmentType.valueOf(args[0].toUpperCase());

            switch (assignmentType) {
                case A1 -> {
                    System.out.println("------- Assignment 1 -------");
                    Assignment1.executeAssignment1();
                }
                default -> {
                    System.err.println("Invalid assignment provided: " + args[0]);
                    return;
                }
            }
        } catch (Exception e) {
            System.err.println("Could not identify correct assignment type for: " + args[0]);
        }
    }
}
