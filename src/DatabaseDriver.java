import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by sonia.marginean on 3/9/16.
 */
public class DatabaseDriver {
    private static final String dbDriver = "com.mysql.jdbc.Driver";
    private static final String dbConnection = "jdbc:mysql://";
    private static final String username = "";
    private static final String password = "";

    private static final String query1 = "SELECT * FROM benchmark WHERE benchmark.columnA = ?";
    private static final String query2 = "SELECT * FROM benchmark WHERE benchmark.columnB = ?";
    private static final String query3 = "SELECT * FROM benchmark WHERE benchmark.columnA = ? AND benchmark.columnB = ?";

    private static final String setupIndexA  = "CREATE INDEX IndexA on benchmark(columnA)";
    private static final String setupIndexB  = "CREATE INDEX IndexB on benchmark(columnB)";
    private static final String setupIndexAB = "CREATE INDEX IndexAB on benchmark(columnA, columnB)";

    private static final String teardownIndexA  = "ALTER TABLE benchmark DROP INDEX IndexA";
    private static final String teardownIndexB  = "ALTER TABLE benchmark DROP INDEX IndexB";
    private static final String teardownIndexAB = "ALTER TABLE benchmark DROP INDEX IndexAB";

    private static final String insertRecord = "INSERT INTO benchmark VALUES(?, ?, ?, ?)";

    private static ArrayList<Record> benchmarkRecords;

    // Schema:
    // CREATE TABLE benchmark ( theKey NUMBER PRIMARY KEY, columnA NUMBER, columnB NUMBER, filler CHAR(247));
    static class Record {
        private long theKey;
        private long columnA;
        private long columnB;
        private String filler;

        public Record(long key){
            this.theKey = key;
            this.columnA = 0;
            this.columnB = 0;
            this.filler = "";
        }
    }

    static void GenerateRecords(int elems, boolean sorted) {
        benchmarkRecords = new ArrayList<Record>();
        for (int i=0; i<elems; i++){
            benchmarkRecords.add(new Record(i));
        }
        if (!sorted){
            Collections.shuffle(benchmarkRecords);
        }
    }

    public static void InsertRecords(int batchSize){
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            Class.forName(dbDriver);
            connection = DriverManager.getConnection(dbConnection, username, password);
            preparedStatement = connection.prepareStatement(insertRecord);
            connection.setAutoCommit(false);

            int i = 0;
            for (Record record : benchmarkRecords) {
                preparedStatement.setLong(1,record.theKey);
                preparedStatement.setLong(2,record.columnA);
                preparedStatement.setLong(3,record.columnB);
                preparedStatement.setString(4, record.filler);
                preparedStatement.addBatch();
                i++;
                if (i % batchSize == 0 || i == benchmarkRecords.size()) {
                    preparedStatement.executeBatch();
                }
            }
            connection.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                preparedStatement.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void ExecuteQuery1(int value){
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            Class.forName(dbDriver);
            connection = DriverManager.getConnection(dbConnection, username, password);
            preparedStatement = connection.prepareStatement(query1);
            preparedStatement.setInt(1,value);
            rs = preparedStatement.executeQuery();
            while (rs.next());
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                preparedStatement.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void ExecuteQuery2(int value){
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            Class.forName(dbDriver);
            connection = DriverManager.getConnection(dbConnection, username, password);
            preparedStatement = connection.prepareStatement(query2);
            preparedStatement.setInt(1,value);
            rs = preparedStatement.executeQuery();
            while (rs.next());
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                preparedStatement.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void ExecuteQuery3(int value){
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            Class.forName(dbDriver);
            connection = DriverManager.getConnection(dbConnection, username, password);
            preparedStatement = connection.prepareStatement(query3);
            preparedStatement.setInt(1,value);
            preparedStatement.setInt(2,value);
            rs = preparedStatement.executeQuery();
            while (rs.next());
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                preparedStatement.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void SetupSecondaryIndexes(boolean indexA, boolean indexB){
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        String queryStmt = null;
        if (indexA & indexB){
            queryStmt = setupIndexAB;
        } else if (indexA) {
            queryStmt = setupIndexA;
        } else if (indexB) {
            queryStmt = setupIndexB;
        }
        if(queryStmt != null){
            try {
                Class.forName(dbDriver);
                connection = DriverManager.getConnection(dbConnection, username, password);
                preparedStatement = connection.prepareStatement(queryStmt);
                if(indexA) {
                    preparedStatement.setBoolean(1, indexA);
                    if(indexB){
                        preparedStatement.setBoolean(2, indexB);
                    }
                } else {
                    preparedStatement.setBoolean(1, indexB);
                }
                preparedStatement.executeUpdate();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    preparedStatement.close();
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void TeardownSecondaryIndexes(boolean indexA, boolean indexB){
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        String queryStmt = null;
        if (indexA & indexB){
            queryStmt = teardownIndexAB;
        } else if (indexA) {
            queryStmt = teardownIndexA;
        } else if (indexB) {
            queryStmt = teardownIndexB;
        }
        if(queryStmt != null){
            try {
                Class.forName(dbDriver);
                connection = DriverManager.getConnection(dbConnection, username, password);
                preparedStatement = connection.prepareStatement(queryStmt);
                if(indexA) {
                    preparedStatement.setBoolean(1, indexA);
                    if(indexB){
                        preparedStatement.setBoolean(2, indexB);
                    }
                } else {
                    preparedStatement.setBoolean(1, indexB);
                }
                preparedStatement.executeUpdate();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    preparedStatement.close();
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
