/**
 * Created by sonia.marginean on 3/9/16.
 */
public class DatabaseDriverTest {
    static int[] _testValues = {5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000, 45000, 50000};

    public static void main(String[] args) {
        // test insertion sorted
        DatabaseDriver.GenerateRecords(1000, true);
        System.out.println("Sorted insertion takes : " + TestInsertion(100) + "ms");

        // test insertion not sorted
        DatabaseDriver.GenerateRecords(1000, false);
        System.out.println("Unsorted insertion takes : " + TestInsertion(100) + "ms");

        //test query 1
        System.out.println("Avg query 1 with no secondary indexes takes : " + AverageQuery1(false, false) + "ms");
        System.out.println("Avg query 1 with columnA secondary index takes : " + AverageQuery1(true, false) + "ms");
        System.out.println("Avg query 1 with columnB secondary index takes : " + AverageQuery1(false, true) + "ms");
        System.out.println("Avg query 1 with both secondary indexes takes : " + AverageQuery1(true, true) + "ms");

        //test query 2
        System.out.println("Avg query 2 with no secondary indexes takes : " + AverageQuery2(false, false) + "ms");
        System.out.println("Avg query 2 with columnA secondary index takes : " + AverageQuery2(true, false) + "ms");
        System.out.println("Avg query 2 with columnB secondary index takes : " + AverageQuery2(false, true) + "ms");
        System.out.println("Avg query 2 with both secondary indexes takes : " + AverageQuery2(true, true) + "ms");

        //test query 3
        System.out.println("Avg query 3 with no secondary indexes takes : " + AverageQuery3(false, false) + "ms");
        System.out.println("Avg query 3 with columnA secondary index takes : " + AverageQuery3(true, false) + "ms");
        System.out.println("Avg query 3 with columnB secondary index takes : " + AverageQuery3(false, true) + "ms");
        System.out.println("Avg query 3 with both secondary indexes takes : " + AverageQuery3(true, true) + "ms");
    }

    public static long TestInsertion(int elems){
        long timeInMs = 0;
        long now = System.currentTimeMillis();
        DatabaseDriver.InsertRecords(elems);
        long then = System.currentTimeMillis();
        timeInMs += then-now;
        return timeInMs;
    }

    // Query 1
    // SELECT * FROM benchmark
    // WHERE benchmark.columnA = 25000
    public static long AverageQuery1(boolean indexA, boolean indexB){
        DatabaseDriver.SetupSecondaryIndexes(indexA, indexB);
        long avgTimeInMs = 0;
        for (int i=0; i<10; i++){
            long now = System.currentTimeMillis();
            DatabaseDriver.ExecuteQuery1(_testValues[i]);
            long then = System.currentTimeMillis();
            avgTimeInMs += then-now;
        }
        DatabaseDriver.TeardownSecondaryIndexes(indexA, indexB);
        return avgTimeInMs/10;
    }

    // Query 2
    // SELECT * FROM benchmark
    // WHERE benchmark.columnB = 25000
    public static long AverageQuery2(boolean indexA, boolean indexB){
        DatabaseDriver.SetupSecondaryIndexes(indexA, indexB);
        long avgTimeInMs = 0;
        for (int i=0; i<10; i++){
            long now = System.currentTimeMillis();
            DatabaseDriver.ExecuteQuery2(_testValues[i]);
            long then = System.currentTimeMillis();
            avgTimeInMs += then-now;
        }
        DatabaseDriver.TeardownSecondaryIndexes(indexA, indexB);
        return avgTimeInMs/10;
    }

    // Query 3
    // SELECT * FROM benchmark
    // WHERE benchmark.columnA = 25000
    // AND benchmark.columnB = 25000
    public static long AverageQuery3(boolean indexA, boolean indexB){
        DatabaseDriver.SetupSecondaryIndexes(indexA, indexB);
        long avgTimeInMs = 0;
        for (int i=0; i<10; i++){
            long now = System.currentTimeMillis();
            DatabaseDriver.ExecuteQuery3(_testValues[i]);
            long then = System.currentTimeMillis();
            avgTimeInMs += then-now;
        }
        DatabaseDriver.TeardownSecondaryIndexes(indexA, indexB);
        return avgTimeInMs/10;
    }
}
