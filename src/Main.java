import java.sql.*;
import java.util.*;
import java.util.stream.IntStream;

public class Main {
    private static boolean listCompare(List<HashMap<String, String>> l1, List<HashMap<String, String>> l2) {
        ArrayList<Boolean> list = new ArrayList<>();
        int firstSize = l1.size();
        int secondSize = l2.size();
        if (firstSize != secondSize) {
            return false;
        }
        IntStream stream = IntStream.range(0, l1.size());
        stream.forEach(i -> list.add(l2.get(i).equals(l1.get(i))));
        boolean o = list.stream()
                .allMatch(n -> n == true);
        return o;


    }

    public static void main(String[] args) throws SQLException {
        Scanner in = new Scanner(System.in);

        System.out.print("Input first database's name: ");
        String firstName = in.nextLine();
        System.out.print("Input second database's name: ");
        String secondName = in.nextLine();
        System.out.print("Input first database's user's name: ");
        String firsUser = in.nextLine();
        System.out.print("Input second database's user's name: ");
        String secondUser = in.nextLine();
        System.out.print("Input first database's user's password: ");
        String firstPassword = in.nextLine();
        System.out.print("Input second database's user's password: ");
        String secondPassword = in.nextLine();
        System.out.print("Input first table's name: ");
        String firstTable = in.nextLine();
        System.out.print("Input second table's name: ");
        String secondTable = in.nextLine();

        System.out.println("Column names are: ");
        String columnNames = in.nextLine();
        List<String> column = Arrays.asList(columnNames.split(","));
        System.out.println("Ready to work...");

        String firstUlr = "jdbc:postgresql://localhost:5432/" + firstName;

        String secondUlr = "jdbc:postgresql://localhost:5432/" + secondName;


        Connection firstConnection = null;
        Connection secondConnection = null;
        try {
            firstConnection = DriverManager.getConnection(
                    firstUlr,
                    firsUser, firstPassword);
            secondConnection = DriverManager.getConnection(
                    secondUlr,
                    secondUser, secondPassword);

            if (firstConnection == null || secondConnection == null) {
                System.out.println("Нет соединения с БД!");
                System.exit(0);
            }

            Statement firstStatement = firstConnection.createStatement();
            Statement secondStatement = secondConnection.createStatement();


            int size = column.size();
            ResultSet firstNumber = firstStatement.executeQuery("SELECT COUNT(*) FROM " + firstTable);
            ResultSet secondNumber = secondStatement.executeQuery("SELECT COUNT(*) FROM " + secondTable);
            List<HashMap<String, String>> list1 = new ArrayList<>();
            List<HashMap<String, String>> list2 = new ArrayList<>();
            int firstRows = 0;
            int secondRows = 0;
            if (firstNumber.next()) {
                firstRows = firstNumber.getInt("COUNT");
            }
            if (secondNumber.next()) {
                secondRows = secondNumber.getInt("COUNT");
            }
            IntStream firstStream = IntStream.rangeClosed(1, firstRows);
            IntStream secondStream = IntStream.rangeClosed(1, secondRows);

            ResultSet firstResultSet = firstStatement.executeQuery("SELECT " + columnNames + " FROM " + firstTable);
            ResultSet secondResultSet = secondStatement.executeQuery("SELECT " + columnNames + " FROM " + secondTable);
            firstStream.forEach(i ->
            {
                try {
                    if (firstResultSet.next()) {
                        HashMap<String, String> object1 = new HashMap<>();
                        list1.add(object1);
                        IntStream stream = IntStream.range(0, size);
                        stream.forEach(o ->
                        {

                            try {
                                object1.put(column.get(o), firstResultSet.getString(o + 1));
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }

                        });
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
            secondStream.forEach(i ->
            {
                try {
                    if (secondResultSet.next()) {
                        HashMap<String, String> object2 = new HashMap<>();
                        list2.add(object2);
                        IntStream stream = IntStream.range(0, size);
                        stream.forEach(o ->
                        {

                            try {
                                object2.put(column.get(o), secondResultSet.getString(o + 1));
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }

                        });
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
            System.out.println(listCompare(list1, list2));

            firstStatement.close();
            secondStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (firstConnection != null) {
                firstConnection.close();

            }
            if (secondConnection != null) {
                secondConnection.close();

            }
        }
    }
}
