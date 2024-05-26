package edu.plus.cs.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class BenchmarkLogger {
    private String fileName;

    public BenchmarkLogger(String fileName) {
        this.fileName = fileName;
    }

    public void log(String indexType, int executedQueries1, int executedQueries2, int executedQueries3, int executedQueries4) {
        String message = String.format("%s: %d, %d, %d, %d\n",
                indexType, executedQueries1, executedQueries2, executedQueries3, executedQueries4);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true))) {
            writer.write(message);
            writer.flush();
        } catch (IOException e) {
            System.err.println("Error writing to the benchmark log file!");
            e.printStackTrace();
        }
    }
}