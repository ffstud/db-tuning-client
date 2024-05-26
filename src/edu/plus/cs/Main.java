package edu.plus.cs;

import edu.plus.cs.assignment.Assignment1;
import edu.plus.cs.assignment.Assignment2;
import edu.plus.cs.assignment.Assignment4;
import edu.plus.cs.util.AssignmentType;

public class Main {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Invalid call, no assignment given");
        }

        AssignmentType assignmentType;
        try {
            assignmentType = AssignmentType.valueOf(args[0].toUpperCase());

            switch (assignmentType) {
                case A1 -> {
                    System.out.println("------- Assignment 1 -------");
                    Assignment1.executeAssignment1();
                    break;
                }
                case A2 -> {
                    System.out.println("------- Assignment 2 -------");
                    Assignment2.executeAssignment2();
                    break;
                }
                case A4 -> {
                    System.out.println("------- Assignment 4 -------");
                    Assignment4.executeAssignment4();
                    break;
                }
                default -> {
                    System.err.println("Invalid assignment provided: " + args[0]);
                    return;
                }
            }
        } catch (Exception e) {
            System.err.println("Could not identify correct assignment type for: " + args[0]);
            System.err.println(e.getMessage());
        }
    }
}
