package application;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Engine implements Runnable {

    private Connection connection;

    public Engine(Connection connection) {
        this.connection = connection;
    }

    public void run() {

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            this.addMinion();
            //comments below are meant for QA department
            //to start task 2 change row above with this: this.getVillainsNames();
            //to start task 3 change row above with this: this.getVillainNameAndMinions(Integer.parseInt(reader.readLine()));
            //to start task 4 change row above with this: this.addMinion();
            //to start task 5 change row above with this: this.changeTownNamesCasing(reader.readLine());
            //to start task 7 change row above with this: this.printAllMinionNames();
            //to start task 8 change row above with this: this.increaseMinionsAge(reader.readLine());
            //to start task 9 change row above with this: this.increaseAgeStoredProcedure(Integer.parseInt(reader.readLine()));
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Problem 2. Get Villains' Names
     * Write a program that prints on the console all villains’ names and their number of minions.
     * Get only the villains who have more than 3 minions. Order them by number of minions in descending order.
     */
    private void getVillainsNames(String s) throws SQLException, IOException {

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        String query = "SELECT v.name, count(mv.minion_id) FROM villains v JOIN minions_villains mv  ON v.id = mv.villain_id GROUP BY v.name HAVING count(mv.minion_id)> ? ORDER BY count(mv.minion_id) DESC";

        PreparedStatement preparedStatement = this.connection.prepareStatement(query);
        preparedStatement.setInt(1, Integer.parseInt(reader.readLine()));

        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()){

            System.out.printf("%s %d%n", resultSet.getString(1),resultSet.getInt(2));
        }

    }

    /**
     * 3.	Get Minion Names
     * Write a program that prints on the console all minion names and their age for given villain id.
     * For the output, use the formats given in the examples.
     * @param villainId
     */

    private void getVillainNameAndMinions(int villainId) throws SQLException {


        String query = String.format("SELECT v.name, m.name, m.age FROM minions m JOIN minions_villains m2 ON m.id = " +
                "m2.minion_id RIGHT JOIN villains v ON m2.villain_id = v.id WHERE v.id = %d", villainId);
        PreparedStatement preparedStatement = this.connection.prepareStatement(query);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (!resultSet.next()) {
            System.out.printf("No villain with ID %d exists in the database.", villainId);
        } else if (resultSet.getString(2) == null) {
            System.out.printf("Villain: %s", resultSet.getString(1));
        } else {
            System.out.printf("Villain: %s%n", resultSet.getString(1));
            int index = 1;
            do {
                System.out.printf("%d. %s %d%n", index, resultSet.getString(2), resultSet.getInt(3));
                index++;
            } while (resultSet.next());
        }

    }

    /**
     * 4.	Add Minion
     * Write a program that reads information about a minion and its villain and adds it to the database.
     * In case the town of the minion is not in the database, insert it as well.
     * In case the villain is not present in the database, add him too with default evilness factor of “evil”.
     * Finally, set the new minion to be servant of the villain.
     * Print appropriate messages after each operation – see the examples.
     */

    private void addMinion() throws IOException, SQLException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        String[] minionData = reader.readLine().split("\\s+");
        String[] villainData = reader.readLine().split("\\s+");

        String minionName = minionData[1];
        int minionAge = Integer.parseInt(minionData[2]);
        String town = minionData[3];

        String villainName = villainData[1];

        if (!checkIfExistTown(town)) {
            String query = String.format("INSERT INTO towns(name, country) VALUES('%s',NULL)", town);
            PreparedStatement preparedStatement = this.connection.prepareStatement(query);
            preparedStatement.executeUpdate();
            System.out.printf("Town %s was added to the database.%n", town);
        }
        if (!checkIfExistVillain(villainName)) {
            String query = String.format("INSERT INTO villains(name, evilness_factor) VALUES('%s','evil')", villainName);
            PreparedStatement preparedStatement = this.connection.prepareStatement(query);
            preparedStatement.executeUpdate();
            System.out.printf("Villain %s was added to the database.%n", villainName);
        }
        int townId = this.checkId(town, "towns");

        String query = String.format("INSERT INTO minions(name, age, town_id) VALUES('%s',%d,%d)", minionName, minionAge, townId);
        PreparedStatement preparedStatement = this.connection.prepareStatement(query);
        preparedStatement.executeUpdate();

        int minionID = this.checkId(minionName, "minions");
        int villainID = this.checkId(villainName, "villains");
        String query1 = String.format("INSERT INTO minions_villains(minion_id,villain_id) VALUES(%d,%d)", minionID, villainID);
        PreparedStatement ps = this.connection.prepareStatement(query1);
        ps.executeUpdate();
        System.out.printf("Successfully added %s to be minion of %s%n", minionName, villainName);
    }

    private boolean checkIfExistTown(String columnName) throws SQLException {
        String query = String.format("SELECT * FROM towns WHERE name = '%s'", columnName);
        PreparedStatement preparedStatement = this.connection.prepareStatement(query);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (!resultSet.next()) {
            return false;
        }
        return true;
    }

    private boolean checkIfExistVillain(String villainName) throws SQLException {
        String query = String.format("SELECT * FROM villains WHERE name = '%s'", villainName);
        PreparedStatement preparedStatement = this.connection.prepareStatement(query);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (!resultSet.next()) {
            return false;
        }
        return true;
    }

    private int checkId(String name, String tableName) throws SQLException {
        String query = String.format("SELECT t.id FROM %s t WHERE t.name = '%s'", tableName, name);
        PreparedStatement preparedStatement = this.connection.prepareStatement(query);
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        return resultSet.getInt(1);
    }



}
