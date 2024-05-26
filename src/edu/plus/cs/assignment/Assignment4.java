package edu.plus.cs.assignment;

import edu.plus.cs.model.Publication;
import edu.plus.cs.util.BenchmarkLogger;
import edu.plus.cs.util.DbUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class Assignment4 {

    private static final String AUTH_FILE = "auth.tsv";
    private static final String PUBL_FILE = "publ.tsv";

    public static void executeAssignment4() {
        Optional<Connection> optionalConnection = DbUtils.connectToPostgres("db-tuning-1");

        if (optionalConnection.isEmpty()) {
            System.err.println("Could not establish connection to postgres");
            return;
        }

        Connection connection = optionalConnection.get();
        try {
            // create tables
            DbUtils.removeTableIfExists(connection, "public.auth");
            DbUtils.removeTableIfExists(connection, "public.publ");

            // auth table
            String createTableSql = "CREATE TABLE IF NOT EXISTS public.auth (name character varying(49), pubid character varying(149))";
            Statement createTableStatement = connection.createStatement();

            createTableStatement.execute(createTableSql);
            createTableStatement.close();

            // publ table
            createTableSql = "CREATE TABLE IF NOT EXISTS public.publ (pubid character varying(129), type character varying(13), " +
                    "title character varying(700), booktitle character varying(132), year character varying(4), " +
                    "publisher character varying(196))";
            createTableStatement = connection.createStatement();

            createTableStatement.execute(createTableSql);
            createTableStatement.close();

            // insert data into tables with batch import (to save the necessary parameter for the queries)
            List<String> authors = insertAuthBatch(connection, AUTH_FILE);
            List<Publication> publications = insertPublBatch(connection, PUBL_FILE);

            // prepare logging of benchmarks
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
            String formattedDateTime = LocalDateTime.now().format(formatter);
            String fileName = "benchmark_log_" + formattedDateTime + ".txt";
            BenchmarkLogger benchmarkLogger = new BenchmarkLogger(fileName);

            // clustering b+ tree index
            executeClusteringBTreeIndexBenchmark(connection, publications, authors, benchmarkLogger);
            executeNonClusteringBTreeIndexBenchmark(connection, publications, authors, benchmarkLogger);
            executeNonClusteringHashIndexBenchmark(connection, publications, authors, benchmarkLogger);
            executeNoIndexBenchmark(connection, publications, authors, benchmarkLogger);

            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void executeClusteringBTreeIndexBenchmark(Connection connection, List<Publication> publications,
                                                             List<String> authors, BenchmarkLogger benchmarkLogger) throws SQLException {
        // ---------------- point query
        // create index on pubid
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE INDEX idx_clustering_pubid ON public.publ (pubid)");

        Statement createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // cluster index after pubid
        stringBuilder.append("CLUSTER public.publ USING idx_clustering_pubid");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // analyze table
        stringBuilder.append("ANALYZE public.publ");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        int executedQueries1 = executeBenchmarkQuery1(connection, publications, authors, benchmarkLogger);

        stringBuilder.append("DROP INDEX idx_clustering_pubid");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // ---------------- multipoint query, low selectivity

        // create index on booktitle
        stringBuilder.append("CREATE INDEX idx_clustering_booktitle ON public.publ (booktitle)");

        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // cluster index after booktitle
        stringBuilder.append("CLUSTER public.publ USING idx_clustering_booktitle");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // analyze table
        stringBuilder.append("ANALYZE public.publ");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        int executedQueries2 = executeBenchmarkQuery2(connection, publications, authors, benchmarkLogger);

        stringBuilder.append("DROP INDEX idx_clustering_booktitle");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // ---------------- multipoint query, in predicate, low selectivity
        // create index on pubid
        stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE INDEX idx_clustering_pubid ON public.publ (pubid)");

        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // cluster index after pubid
        stringBuilder.append("CLUSTER public.publ USING idx_clustering_pubid");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // analyze table
        stringBuilder.append("ANALYZE public.publ");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // create index on name
        stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE INDEX idx_clustering_name ON public.auth (name)");

        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // cluster index after pubid
        stringBuilder.append("CLUSTER public.auth USING idx_clustering_name");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // analyze table
        stringBuilder.append("ANALYZE public.auth");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        int executedQueries3 = executeBenchmarkQuery3(connection, publications, authors, benchmarkLogger);

        stringBuilder.append("DROP INDEX idx_clustering_pubid");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        stringBuilder.append("DROP INDEX idx_clustering_name");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // ---------------- multipoint query, high selectivity
        // create index on year
        stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE INDEX idx_clustering_year ON public.publ (year)");

        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // cluster index after pubid
        stringBuilder.append("CLUSTER public.publ USING idx_clustering_year");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // analyze table
        stringBuilder.append("ANALYZE public.publ");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        int executedQueries4 = executeBenchmarkQuery4(connection, publications, authors, benchmarkLogger);

        stringBuilder.append("DROP INDEX idx_clustering_year");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        benchmarkLogger.log("Clustering B+ Tree Index", executedQueries1, executedQueries2, executedQueries3,
                executedQueries4);
    }

    private static void executeNonClusteringBTreeIndexBenchmark(Connection connection, List<Publication> publications,
                                                             List<String> authors, BenchmarkLogger benchmarkLogger) throws SQLException {
        // ---------------- point query
        // create index on pubid
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE INDEX idx_clustering_pubid ON public.publ (pubid)");

        Statement createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // analyze table
        stringBuilder.append("ANALYZE public.publ");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        int executedQueries1 = executeBenchmarkQuery1(connection, publications, authors, benchmarkLogger);

        stringBuilder.append("DROP INDEX idx_clustering_pubid");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // ---------------- multipoint query, low selectivity

        // create index on booktitle
        stringBuilder.append("CREATE INDEX idx_clustering_booktitle ON public.publ (booktitle)");

        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // analyze table
        stringBuilder.append("ANALYZE public.publ");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        int executedQueries2 = executeBenchmarkQuery2(connection, publications, authors, benchmarkLogger);

        stringBuilder.append("DROP INDEX idx_clustering_booktitle");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // ---------------- multipoint query, in predicate, low selectivity
        // create index on pubid
        stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE INDEX idx_clustering_pubid ON public.publ (pubid)");

        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // analyze table
        stringBuilder.append("ANALYZE public.publ");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // create index on name
        stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE INDEX idx_clustering_name ON public.auth (name)");

        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // analyze table
        stringBuilder.append("ANALYZE public.auth");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        int executedQueries3 = executeBenchmarkQuery3(connection, publications, authors, benchmarkLogger);

        stringBuilder.append("DROP INDEX idx_clustering_pubid");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        stringBuilder.append("DROP INDEX idx_clustering_name");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // ---------------- multipoint query, high selectivity
        // create index on year
        stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE INDEX idx_clustering_year ON public.publ (year)");

        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // analyze table
        stringBuilder.append("ANALYZE public.publ");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        int executedQueries4 = executeBenchmarkQuery4(connection, publications, authors, benchmarkLogger);

        stringBuilder.append("DROP INDEX idx_clustering_year");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        benchmarkLogger.log("Non-Clustering B+ Tree Index", executedQueries1, executedQueries2, executedQueries3,
                executedQueries4);
    }

    private static void executeNonClusteringHashIndexBenchmark(Connection connection, List<Publication> publications,
                                                                List<String> authors, BenchmarkLogger benchmarkLogger) throws SQLException {
        // ---------------- point query
        // create index on pubid
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE INDEX idx_clustering_pubid ON public.publ USING HASH (pubid)");

        Statement createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // analyze table
        stringBuilder.append("ANALYZE public.publ");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        int executedQueries1 = executeBenchmarkQuery1(connection, publications, authors, benchmarkLogger);

        stringBuilder.append("DROP INDEX idx_clustering_pubid");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // ---------------- multipoint query, low selectivity

        // create index on booktitle
        stringBuilder.append("CREATE INDEX idx_clustering_booktitle ON public.publ USING HASH (booktitle)");

        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // analyze table
        stringBuilder.append("ANALYZE public.publ");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        int executedQueries2 = executeBenchmarkQuery2(connection, publications, authors, benchmarkLogger);

        stringBuilder.append("DROP INDEX idx_clustering_booktitle");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // ---------------- multipoint query, in predicate, low selectivity
        // create index on pubid
        stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE INDEX idx_clustering_pubid ON public.publ USING HASH (pubid)");

        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // analyze table
        stringBuilder.append("ANALYZE public.publ");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // create index on name
        stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE INDEX idx_clustering_name ON public.auth USING HASH (name)");

        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // analyze table
        stringBuilder.append("ANALYZE public.auth");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        int executedQueries3 = executeBenchmarkQuery3(connection, publications, authors, benchmarkLogger);

        stringBuilder.append("DROP INDEX idx_clustering_pubid");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        stringBuilder.append("DROP INDEX idx_clustering_name");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // ---------------- multipoint query, high selectivity
        // create index on year
        stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE INDEX idx_clustering_year ON public.publ USING HASH (year)");

        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // analyze table
        stringBuilder.append("ANALYZE public.publ");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        int executedQueries4 = executeBenchmarkQuery4(connection, publications, authors, benchmarkLogger);

        stringBuilder.append("DROP INDEX idx_clustering_year");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        benchmarkLogger.log("Non-Clustering Hash Index", executedQueries1, executedQueries2, executedQueries3,
                executedQueries4);
    }

    private static void executeNoIndexBenchmark(Connection connection, List<Publication> publications,
                                                               List<String> authors, BenchmarkLogger benchmarkLogger) throws SQLException {
        // ---------------- point query
        StringBuilder stringBuilder = new StringBuilder();

        // analyze table
        stringBuilder.append("ANALYZE public.publ");
        Statement createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        int executedQueries1 = executeBenchmarkQuery1(connection, publications, authors, benchmarkLogger);

        // ---------------- multipoint query, low selectivity

        // analyze table
        stringBuilder.append("ANALYZE public.publ");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        int executedQueries2 = executeBenchmarkQuery2(connection, publications, authors, benchmarkLogger);

        // ---------------- multipoint query, in predicate, low selectivity

        // analyze table
        stringBuilder.append("ANALYZE public.publ");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        // analyze table
        stringBuilder.append("ANALYZE public.auth");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        int executedQueries3 = executeBenchmarkQuery3(connection, publications, authors, benchmarkLogger);

        // ---------------- multipoint query, high selectivity

        // analyze table
        stringBuilder.append("ANALYZE public.publ");
        createIndexStatement = connection.createStatement();
        createIndexStatement.execute(stringBuilder.toString());

        stringBuilder.setLength(0);

        int executedQueries4 = executeBenchmarkQuery4(connection, publications, authors, benchmarkLogger);

        benchmarkLogger.log("Table Scan", executedQueries1, executedQueries2, executedQueries3,
                executedQueries4);
    }

    private static int executeBenchmarkQuery1(Connection connection, List<Publication> publications, List<String> authors,
                                               BenchmarkLogger benchmarkLogger) throws SQLException {
        int executedQueries1 = 0;
        long currentTimeInMs;
        long targetTimeInMs;

        // point query
        currentTimeInMs = System.currentTimeMillis();
        targetTimeInMs = currentTimeInMs + 60000;

        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM public.publ WHERE pubid = ?");

        Random random = new Random();
        while (System.currentTimeMillis() < targetTimeInMs) {
            preparedStatement.setString(1, publications.get(random.nextInt(0, publications.size())).getPubid());
            ResultSet resultSet = preparedStatement.executeQuery();

            // if (executedQueries % 10000 == 0) {
            //     resultSet.next();
            //     System.out.println(resultSet.getString("pubid"));
            // }

            if (System.currentTimeMillis() < targetTimeInMs) {
                executedQueries1++;
            }
        }

        return executedQueries1;
    }

    private static int executeBenchmarkQuery2(Connection connection, List<Publication> publications, List<String> authors,
                                               BenchmarkLogger benchmarkLogger) throws SQLException {
        int executedQueries2 = 0;
        long currentTimeInMs;
        long targetTimeInMs;

        // multipoint query, low selectivity
        currentTimeInMs = System.currentTimeMillis();
        targetTimeInMs = currentTimeInMs + 60000;

        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM public.publ WHERE booktitle = ?");

        List<String> nonEmptyBookTitles = publications.stream().filter(publication -> !publication.getBookTitle().isEmpty())
                .map(publication -> publication.getBookTitle()).collect(Collectors.toList());

        Random random = new Random();
        while (System.currentTimeMillis() < targetTimeInMs) {
            preparedStatement.setString(1, nonEmptyBookTitles.get(random.nextInt(0, nonEmptyBookTitles.size())));
            ResultSet resultSet = preparedStatement.executeQuery();

            // if (executedQueries2 % 10000 == 0) {
            //     resultSet.next();
            //     System.out.println(resultSet.getString("booktitle"));
            // }

            if (System.currentTimeMillis() < targetTimeInMs) {
                executedQueries2++;
            }
        }

        return executedQueries2;
    }

    private static int executeBenchmarkQuery3(Connection connection, List<Publication> publications, List<String> authors,
                                               BenchmarkLogger benchmarkLogger) throws SQLException {
        int executedQueries3 = 0;
        long currentTimeInMs;
        long targetTimeInMs;

        // multipoint query with in predicate
        currentTimeInMs = System.currentTimeMillis();
        targetTimeInMs = currentTimeInMs + 60000;

        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM public.publ WHERE pubid IN (SELECT pubid FROM public.auth WHERE name IN (?, ?, ?))");

        Random random = new Random();
        // HashSet<String> uniqueAuthors;
        while (System.currentTimeMillis() < targetTimeInMs) {
            // uniqueAuthors = new HashSet<>();
            // while (uniqueAuthors.size() < 3) {
            //     uniqueAuthors.add(authors.get(random.nextInt(0, authors.size())));
            // }
            //
            // String[] authorsArray = new String[3];
            // uniqueAuthors.toArray(authorsArray);

            preparedStatement.setString(1, authors.get(random.nextInt(0, authors.size())));
            preparedStatement.setString(2, authors.get(random.nextInt(0, authors.size())));
            preparedStatement.setString(3, authors.get(random.nextInt(0, authors.size())));
            ResultSet resultSet = preparedStatement.executeQuery();

            // if (executedQueries % 10000 == 0) {
            //     resultSet.next();
            //     System.out.println(resultSet.getString("pubid"));
            // }

            if (System.currentTimeMillis() < targetTimeInMs) {
                executedQueries3++;
            }
        }

        return executedQueries3;
    }

    private static int executeBenchmarkQuery4(Connection connection, List<Publication> publications, List<String> authors,
                                               BenchmarkLogger benchmarkLogger) throws SQLException {
        int executedQueries4 = 0;
        long currentTimeInMs;
        long targetTimeInMs;

        // multipoint query, high selectivity
        currentTimeInMs = System.currentTimeMillis();
        targetTimeInMs = currentTimeInMs + 60000;

        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM public.publ WHERE year = ?");

        Random random = new Random();
        while (System.currentTimeMillis() < targetTimeInMs) {
            preparedStatement.setString(1, publications.get(random.nextInt(0, publications.size())).getYear());
            ResultSet resultSet = preparedStatement.executeQuery();

            // if (executedQueries % 10000 == 0) {
            //     resultSet.next();
            //     System.out.println(resultSet.getString("pubid"));
            // }

            if (System.currentTimeMillis() < targetTimeInMs) {
                executedQueries4++;
            }
        }

        return executedQueries4;
    }

    private static List<String> insertAuthBatch(Connection connection, String authFile) {
        List<String> authors = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(authFile))) {
            connection.setAutoCommit(false);
            String line;
            PreparedStatement insertAuthorStatement = connection.prepareStatement("INSERT INTO public.auth (name, pubid) VALUES(?, ?)");
            while ((line = br.readLine()) != null) {
                String[] lineParts = line.split("\t");

                authors.add(lineParts[0]);

                if (lineParts.length != 2) {
                    System.err.println("Read line not formatted correctly, got: " + line);
                    return null;
                }

                insertAuthorStatement.setString(1, lineParts[0]);
                insertAuthorStatement.setString(2, lineParts[1]);
                insertAuthorStatement.addBatch();
            }
            insertAuthorStatement.executeBatch();
            connection.commit();

            insertAuthorStatement.close();

            connection.setAutoCommit(true);

            return authors;
        } catch (IOException e) {
            System.err.println("Error reading from input file: " + e.getMessage());
            return null;
        } catch (SQLException e) {
            System.err.println("Error while executing sql");
            throw new RuntimeException(e);
        }
    }

    private static List<Publication> insertPublBatch(Connection connection, String publFile) {
        List<Publication> publications = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(publFile))) {
            connection.setAutoCommit(false);
            String line;
            PreparedStatement insertPublicationStatement = connection.prepareStatement("INSERT INTO public.publ (pubid, type, title, booktitle, year, publisher) VALUES(?, ?, ?, ?, ?, ?)");
            while ((line = br.readLine()) != null) {
                String[] lineParts = line.split("\t");

                Publication publication = new Publication();
                publication.setPubid(lineParts[0]);
                if (lineParts.length > 1) publication.setType(lineParts[1]);
                if (lineParts.length > 2) publication.setTitle(lineParts[2]);
                publication.setBookTitle((lineParts.length > 3) ? lineParts[3] : "");
                if (lineParts.length > 4) publication.setYear(lineParts[4]);
                if (lineParts.length > 5) publication.setPublisher(lineParts[5]);
                publications.add(publication);

                if (lineParts.length > 6) {
                    System.err.println("Read line not formatted correctly, got: " + line);
                    return null;
                }

                insertPublicationStatement.setString(1, lineParts[0]);
                insertPublicationStatement.setString(2, (lineParts.length > 1) ? lineParts[1] : "");
                insertPublicationStatement.setString(3, (lineParts.length > 2) ? lineParts[2] : "");
                insertPublicationStatement.setString(4, (lineParts.length > 3) ? lineParts[3] : "");
                insertPublicationStatement.setString(5, (lineParts.length > 4) ? lineParts[4] : "");
                insertPublicationStatement.setString(6, (lineParts.length > 5) ? lineParts[5] : "");
                insertPublicationStatement.addBatch();
            }
            insertPublicationStatement.executeBatch();
            connection.commit();

            insertPublicationStatement.close();

            connection.setAutoCommit(true);
        } catch (IOException e) {
            System.err.println("Error reading from input file: " + e.getMessage());
            return null;
        } catch (SQLException e) {
            System.err.println("Error while executing sql");
            throw new RuntimeException(e);
        }

        return publications;
    }
}
