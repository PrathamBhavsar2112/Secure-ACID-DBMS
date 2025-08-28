package query;

import interfaces.IQueryProcessor;
import storage.PersistentStorage;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class QueryProcessor implements IQueryProcessor {
    private static final String STORAGE_ROOT = "storage/";
    private final String loggedInUser;
    private String currentDatabase = null;
    private final Scanner scanner = new Scanner(System.in);
    private final PersistentStorage storage;

    private boolean transactionActive = false;
    private final List<String> transactionLog = new ArrayList<>();

    public QueryProcessor(String username) {
        this.loggedInUser = username;
        this.storage = new PersistentStorage();
    }


    @Override
    public void executeQuery(String query) {
        if (query == null || query.trim().isEmpty()) {
            System.out.println("Error: Empty query received.");
            return;
        }

        query = query.trim();
        System.out.println("Executing Query: " + query);

        if (!query.endsWith(";")) {
            System.out.println("Error: SQL queries must end with a semicolon (;).");
            return;
        }

        query = query.substring(0, query.length() - 1).trim();

        String upperQuery = query.toUpperCase();

        if (handleTransactionCommand(query, upperQuery)) {
            return;
        }

        if (upperQuery.equals("BEGIN TRANSACTION")) {
            beginTransaction();
            return;
        } else if (upperQuery.equals("BEGIN")) {
            beginTransaction();
            return;
        }


        String[] queryParts = upperQuery.split(" ", 2);
        String command = queryParts[0];

        switch (command) {
            case "SHOW":
                if (upperQuery.equals("SHOW DATABASES")) {
                    showDatabases();
                } else if (upperQuery.equals("SHOW TABLES")) {
                    showTables();
                } else {
                    System.out.println("Error: Invalid SHOW command. Use 'SHOW DATABASES' or 'SHOW TABLES'.");
                }
                break;
            case "USE":
                useDatabase(query);
                break;
            case "CREATE":
                if (upperQuery.startsWith("CREATE DATABASE")) {
                    createDatabase(query);
                } else if (upperQuery.startsWith("CREATE TABLE")) {
                    createTable(query);
                } else {
                    System.out.println("Error: Invalid CREATE syntax.");
                }
                break;
            case "UPDATE":
                updateData(query);
                break;
            case "DELETE":
                deleteData(query);
                break;
            case "DESCRIBE":
                describeTable(query);
                break;
            case "SELECT":
                selectData(query);
                break;
            case "INSERT":
                insertData(query);
                break;
            case "COMMIT":
                if (!isTransactionActive()) {
                    System.out.println("Error: No active transaction to commit.");
                } else {
                    commitTransaction();
                }
                break;
            case "ROLLBACK":
                if (!isTransactionActive()) {
                    System.out.println("Error: No active transaction to rollback.");
                } else {
                    rollbackTransaction();
                }
                break;
            default:
                System.out.println("Error: Unknown SQL command.");
                break;
        }
    }


    private void showDatabases() {
        File userStorageDir = new File(STORAGE_ROOT + loggedInUser);
        String[] databases = userStorageDir.list((dir, name) -> new File(dir, name).isDirectory());

        if (databases == null || databases.length == 0) {
            System.out.println("No databases found.");
        } else {
            System.out.println("Databases:");
            for (String db : databases) {
                System.out.println("- " + db);
            }
        }
    }
    private void createDatabase(String query) {
        String pattern = "(?i)CREATE\\s+DATABASE\\s+([a-zA-Z0-9_]+)";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(query);

        if (!m.find()) {
            System.out.println("Invalid CREATE DATABASE syntax. Use: CREATE DATABASE db_name;");
            return;
        }

        String dbName = m.group(1);
        if (!dbName.matches("^[a-zA-Z0-9_]+$")) {
            System.out.println("Error: Invalid database name. Use only letters, numbers, or underscores.");
            return;
        }

        File userDir = new File(STORAGE_ROOT + loggedInUser);
        if (!userDir.exists()) {
            userDir.mkdirs();
        }

        String[] existingDatabases = userDir.list((dir, name) -> new File(dir, name).isDirectory());
        if (existingDatabases != null && existingDatabases.length > 0) {
            System.out.println("Error: You can create only one database. Database '" + existingDatabases[0] + "' already exists.");
            return;
        }

        File dbDir = new File(userDir, dbName);
        if (!dbDir.exists()) {
            dbDir.mkdirs();
            currentDatabase = dbName;
            System.out.println("Database '" + dbName + "' created and set as active.");
        } else {
            System.out.println("Database already exists.");
        }
    }

    private void useDatabase(String query) {
        String[] parts = query.split(" ");
        if (parts.length != 2) {
            System.out.println("Invalid USE syntax. Use: USE database_name;");
            return;
        }

        String dbName = parts[1];
        File dbDir = new File(STORAGE_ROOT + loggedInUser + "/" + dbName);


        if (!dbDir.exists() || !dbDir.isDirectory()) {
            System.out.println("Error: Database '" + dbName + "' not found. Create the database first using 'CREATE DATABASE " + dbName + ";'");
            return;
        }

        if (currentDatabase != null) {
            System.out.println("Error: You can only use one database. '" + currentDatabase + "' is already set.");
            return;
        }

        currentDatabase = dbName;
        System.out.println("Database set to: " + dbName);
    }
    private void describeTable(String query) {
        if (currentDatabase == null) {
            System.out.println("No database selected. Use 'USE database_name' first.");
            return;
        }

        String[] parts = query.split("\\s+");
        if (parts.length != 2) {
            System.out.println("Invalid DESCRIBE syntax. Use: DESCRIBE table_name;");
            return;
        }

        String tableName = parts[1].replace(";", "").trim();

        File tableFile = new File(STORAGE_ROOT + loggedInUser + "/" + currentDatabase + "/" + tableName + ".txt");

        if (!tableFile.exists()) {
            System.out.println("Error: Table '" + tableName + "' not found.");
            return;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(tableFile))) {
            String header = br.readLine();
            if (header != null) {
                System.out.println("Table Structure for '" + tableName + "':");
                for (String column : header.split("\\|\\|\\|")) {
                    System.out.println("- " + column);
                }
            } else {
                System.out.println("Table '" + tableName + "' is empty.");
            }
        } catch (IOException e) {
            System.out.println("Error reading table '" + tableName + "': " + e.getMessage());
        }
    }

    private void showTables() {
        if (currentDatabase == null || currentDatabase.isEmpty()) {
            System.out.println("No database selected. Use 'USE database_name' first.");
            return;
        }

        File dbDir = new File(STORAGE_ROOT + loggedInUser + "/" + currentDatabase);
        String[] tables = dbDir.list((dir, name) -> name.endsWith(".txt"));

        if (tables == null || tables.length == 0) {
            System.out.println("No tables found in database: " + currentDatabase);
        } else {
            System.out.println("Tables in database '" + currentDatabase + "':");
            for (String table : tables) {
                System.out.println("- " + table.replace(".txt", ""));
            }
        }
    }

    private void selectData(String query) {
        if (currentDatabase == null || currentDatabase.isEmpty()) {
            System.out.println("No database selected. Use 'USE database_name' first.");
            return;
        }


        query = query.replaceAll("\\s+", " ").trim();
        System.out.println("Received SELECT Query: " + query);

        if (!query.toUpperCase().startsWith("SELECT * FROM ")) {
            System.out.println("Syntax error in SELECT statement. Correct format: SELECT * FROM table_name [WHERE column = value];");
            return;
        }

        String[] queryParts = query.split("\\s+");
        if (queryParts.length < 4 || !"FROM".equalsIgnoreCase(queryParts[2])) {
            System.out.println("Syntax error in SELECT statement. Correct format: SELECT * FROM table_name [WHERE column = value];");
            return;
        }

        String tableName = queryParts[3].replace(";", "").trim();
        System.out.println("Extracted Table Name: " + tableName);

        File tableFile = new File(STORAGE_ROOT + loggedInUser + "/" + currentDatabase + "/" + tableName + ".txt");
        System.out.println("Looking for file: " + tableFile.getAbsolutePath());

        if (!tableFile.exists()) {
            System.out.println("Error: Table '" + tableName + "' not found.");
            return;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(tableFile))) {
            String header = br.readLine();
            if (header == null) {
                System.out.println("Table is empty.");
                return;
            }

            String[] columnNames = header.split("\\|\\|\\|");
            System.out.println("Columns Found: " + String.join(", ", columnNames));
            List<String[]> records = new ArrayList<>();
            boolean hasCondition = query.toUpperCase().contains(" WHERE ");
            int conditionColumnIndex = -1;
            String conditionValue = "";

            if (hasCondition) {
                String conditionPart = query.split("(?i)WHERE")[1].trim();
                String[] conditionParts = conditionPart.split("=");

                if (conditionParts.length != 2) {
                    System.out.println("Syntax error in SELECT statement. Invalid WHERE clause.");
                    return;
                }

                String conditionColumn = conditionParts[0].trim();
                conditionValue = conditionParts[1].trim().replace("'", "").replace("\"", "");

                for (int i = 0; i < columnNames.length; i++) {
                    String pureColumnName = columnNames[i].split(" ")[0].trim();
                    if (pureColumnName.equalsIgnoreCase(conditionColumn)) {
                        conditionColumnIndex = i;
                        break;
                    }
                }

                if (conditionColumnIndex == -1) {
                    System.out.println("Error: Invalid column in WHERE clause: " + conditionColumn);
                    return;
                }
            }

            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split("\\|\\|\\|");

                if (hasCondition && (conditionColumnIndex == -1 || !values[conditionColumnIndex].equalsIgnoreCase(conditionValue))) {
                    continue;
                }
                records.add(values);
            }

            System.out.println("\nData from table '" + tableName + "':");
            System.out.println("-".repeat(50));
            System.out.println(String.join(" | ", columnNames));
            System.out.println("-".repeat(50));

            if (records.isEmpty()) {
                System.out.println("No matching records found.");
            } else {
                for (String[] record : records) {
                    System.out.println(String.join(" | ", record));
                }
            }
            System.out.println("-".repeat(50));

        } catch (IOException e) {
            System.out.println("Error reading table: " + e.getMessage());
        }
    }

    private void createTable(String query) {
        if (currentDatabase == null) {
            System.out.println("No database selected. Use 'USE database_name' first.");
            return;
        }

        String[] parts = query.replaceAll("\\s+", " ").trim().split("\\s+", 3);

        if (parts.length < 3 || !"TABLE".equalsIgnoreCase(parts[1])) {
            System.out.println("Invalid CREATE TABLE syntax. Use: CREATE TABLE table_name (columns);");
            return;
        }

        String tableName = parts[2].split("\\(")[0].trim();

        if (tableName.isEmpty()) {
            System.out.println("Error: Table name missing.");
            return;
        }

        if (!query.contains("(") || !query.contains(")")) {
            System.out.println("Error: Invalid column format in CREATE TABLE.");
            return;
        }

        String columnDefinitions = query.substring(query.indexOf("(") + 1, query.lastIndexOf(")")).trim();
        String[] columnsWithTypes = columnDefinitions.split(",");
        List<String> columnNames = new ArrayList<>();

        for (String colDef : columnsWithTypes) {
            String[] colParts = colDef.trim().split("\\s+");
            if (colParts.length == 0) {
                System.out.println("Error: Invalid column format.");
                return;
            }
            columnNames.add(colParts[0]); // Only keep the column name (remove INT, STRING, etc.)
        }

        File userDbDir = new File(STORAGE_ROOT + loggedInUser + "/" + currentDatabase);
        if (!userDbDir.exists()) {
            userDbDir.mkdirs();
        }

        File tableFile = new File(userDbDir, tableName + ".txt");
        if (tableFile.exists()) {
            System.out.println("Error: Table '" + tableName + "' already exists.");
            return;
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tableFile))) {
            bw.write(String.join("|||", columnNames)); // Write only column names
            bw.newLine();
            System.out.println("Table '" + tableName + "' created successfully.");
        } catch (IOException e) {
            System.out.println("Error creating table: " + e.getMessage());
        }
    }
    
    private void insertData(String query) {
        if (currentDatabase == null) {
            System.out.println("No database selected.");
            return;
        }

        String[] parts = query.replaceAll("\\s+", " ").trim().split(" ", 4);
        if (parts.length < 4 || !"INTO".equalsIgnoreCase(parts[1])) {
            System.out.println("Invalid INSERT syntax. Use: INSERT INTO table_name VALUES (...);");
            return;
        }

        String tableName = parts[2].trim();

        if (!query.contains("(") || !query.contains(")")) {
            System.out.println("Error: Invalid VALUES format in INSERT.");
            return;
        }


        String values = query.substring(query.indexOf("(") + 1, query.lastIndexOf(")")).trim();
        String[] valueList = values.split(",\\s*");

        File tableFile = new File(STORAGE_ROOT + loggedInUser + "/" + currentDatabase + "/" + tableName + ".txt");

        if (!tableFile.exists()) {
            System.out.println("Error: Table '" + tableName + "' not found.");
            return;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(tableFile))) {
            String header = br.readLine();
            if (header == null) {
                System.out.println("Error: Table '" + tableName + "' is empty.");
                return;
            }
            String[] columns = header.split("\\|\\|\\|");

            if (valueList.length != columns.length) {
                System.out.println("Column mismatch: expected " + columns.length + " values but got " + valueList.length + ".");
                return;
            }
        } catch (IOException e) {
            System.out.println("Error reading table '" + tableName + "': " + e.getMessage());
            return;
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tableFile, true))) {
            bw.newLine();
            bw.write(values.replace(", ", "|||"));
            System.out.println("Data inserted into table '" + tableName + "'.");
        } catch (IOException e) {
            System.out.println("Error inserting data: " + e.getMessage());
        }
    }
    private void updateData(String query) {
        if (currentDatabase == null || currentDatabase.isEmpty()) {
            System.out.println("No database selected. Use 'USE database_name' first.");
            return;
        }

        query = query.replaceAll("\\s+", " ").trim();
        System.out.println("Received UPDATE Query: " + query);

        if (!Pattern.matches("UPDATE\\s+\\w+\\s+SET\\s+\\w+\\s*=\\s*('.*?'|\\d+)\\s+WHERE\\s+\\w+\\s*=\\s*('.*?'|\\d+)", query)) {
            System.out.println("Syntax error in UPDATE statement. Use: UPDATE table_name SET column='value' WHERE column=value;");
            return;
        }

        String tablePart = query.split("(?i)UPDATE\\s+")[1].split("(?i)\\s+SET\\s+")[0].trim();
        String setPart = query.split("(?i)\\s+SET\\s+")[1].split("(?i)\\s+WHERE\\s+")[0].trim();
        String wherePart = query.split("(?i)\\s+WHERE\\s+")[1].trim().replace(";", "");

        System.out.println("Table: " + tablePart);
        System.out.println("Set: " + setPart);
        System.out.println("Where: " + wherePart);

        String tableName = tablePart;

        String[] setParts = setPart.split("\\s*=\\s*");
        String updateColumn = setParts[0].trim();
        String updateValue = setParts[1].trim().replace("'", "");
        String[] whereParts = wherePart.split("\\s*=\\s*");
        String whereColumn = whereParts[0].trim();
        String whereValue = whereParts[1].trim().replace("'", "");

        File tableFile = new File(STORAGE_ROOT + loggedInUser + "/" + currentDatabase + "/" + tableName + ".txt");
        System.out.println("Looking for file: " + tableFile.getAbsolutePath());

        if (!tableFile.exists()) {
            System.out.println("Error: Table '" + tableName + "' not found.");
            return;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(tableFile))) {
            List<String> lines = new ArrayList<>();
            String header = br.readLine();
            lines.add(header);
            String[] columns = header.split("\\|\\|\\|");

            int updateColumnIndex = -1, whereColumnIndex = -1;
            for (int i = 0; i < columns.length; i++) {
                if (columns[i].equalsIgnoreCase(updateColumn)) {
                    updateColumnIndex = i;
                }
                if (columns[i].equalsIgnoreCase(whereColumn)) {
                    whereColumnIndex = i;
                }
            }

            if (updateColumnIndex == -1 || whereColumnIndex == -1) {
                System.out.println("Error: Invalid column in UPDATE query.");
                return;
            }

            boolean updated = false;
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split("\\|\\|\\|");

                if (values.length > whereColumnIndex && values[whereColumnIndex].equalsIgnoreCase(whereValue)) {
                    values[updateColumnIndex] = updateValue;
                    updated = true;
                    lines.add(String.join("|||", values));
                } else {
                    lines.add(line);
                }
            }

            if (!updated) {
                System.out.println("No matching record found for update.");
                return;
            }

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(tableFile))) {
                for (String l : lines) {
                    bw.write(l);
                    bw.newLine();
                }
            }

            System.out.println("Record updated successfully in table '" + tableName + "'.");

        } catch (IOException e) {
            System.out.println("Error updating table: " + e.getMessage());
        }
    }

    private void deleteData(String query) {
        if (currentDatabase == null || currentDatabase.isEmpty()) {
            System.out.println("No database selected. Use 'USE database_name' first.");
            return;
        }

        query = query.replaceAll("\\s+", " ").trim();
        System.out.println("Received DELETE Query: " + query);

        if (!Pattern.matches("DELETE\\s+FROM\\s+\\w+\\s+WHERE\\s+\\w+\\s+(?:=|LIKE)\\s+(?:'.*?'|\\d+)", query)) {
            System.out.println("Syntax error in DELETE statement. Use: DELETE FROM table_name WHERE column=value; or DELETE FROM table_name WHERE column LIKE 'pattern%';");
            return;
        }

        String tablePart = query.split("(?i)DELETE\\s+FROM\\s+")[1].split("(?i)\\s+WHERE\\s+")[0].trim();
        String wherePart = query.split("(?i)\\s+WHERE\\s+")[1].trim().replace(";", "");

        System.out.println("Table: " + tablePart);
        System.out.println("Where: " + wherePart);

        String tableName = tablePart;

        boolean isLikeQuery = wherePart.toUpperCase().contains("LIKE");

        String whereColumn, whereValue;

        if (isLikeQuery) {
            whereColumn = wherePart.split("(?i)\\s+LIKE\\s+")[0].trim();
            whereValue = wherePart.split("(?i)\\s+LIKE\\s+")[1].trim();
        } else {
            String[] whereParts = wherePart.split("\\s*=\\s*");
            whereColumn = whereParts[0].trim();
            whereValue = whereParts[1].trim();
        }

        whereValue = whereValue.replaceAll("^'|'$", "");

        System.out.println("Where Column: " + whereColumn);
        System.out.println("Where Value: " + whereValue);
        System.out.println("Is LIKE query: " + isLikeQuery);

        File tableFile = new File(STORAGE_ROOT + loggedInUser + "/" + currentDatabase + "/" + tableName + ".txt");
        System.out.println("Looking for file: " + tableFile.getAbsolutePath());

        if (!tableFile.exists()) {
            System.out.println("Error: Table '" + tableName + "' not found.");
            return;
        }
        try (BufferedReader br = new BufferedReader(new FileReader(tableFile))) {
            List<String> lines = new ArrayList<>();
            String header = br.readLine();
            lines.add(header);
            String[] columns = header.split("\\|\\|\\|");

            int whereColumnIndex = -1;
            for (int i = 0; i < columns.length; i++) {
                if (columns[i].equalsIgnoreCase(whereColumn)) {
                    whereColumnIndex = i;
                    break;
                }
            }

            if (whereColumnIndex == -1) {
                System.out.println("Error: Invalid column in DELETE query.");
                return;
            }

            boolean deleted = false;
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split("\\|\\|\\|");

                if (whereColumnIndex >= values.length) {
                    lines.add(line);
                    continue;
                }

                boolean shouldDelete = false;

                if (isLikeQuery) {
                    String pattern = whereValue.replace("%", ".*");
                    shouldDelete = values[whereColumnIndex].matches(pattern);
                } else {
                    shouldDelete = values[whereColumnIndex].equalsIgnoreCase(whereValue);
                }

                if (shouldDelete) {
                    deleted = true;
                } else {
                    lines.add(line);
                }
            }

            if (!deleted) {
                System.out.println("No matching record found for deletion.");
                return;
            }

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(tableFile))) {
                for (String l : lines) {
                    bw.write(l);
                    bw.newLine();
                }
            }

            System.out.println("Record deleted successfully from table '" + tableName + "'.");

        } catch (IOException e) {
            System.out.println("Error deleting from table: " + e.getMessage());
        }
    }
    private void beginTransaction() {
        if (transactionActive) {
            System.out.println("Error: A transaction is already active.");
            return;
        }
        storage.beginTransaction(loggedInUser);

        transactionActive = true;
        transactionLog.clear();
        System.out.println("BEGIN;");
        System.out.println("Transaction mode is activated.");
    }
    private void commitTransaction() {
        if (!transactionActive) {
            System.out.println("Error: No active transaction to commit.");
            return;
        }

        try {
            for (String query : transactionLog) {
                System.out.println("Applying query: " + query);

            }
            storage.commitTransaction(loggedInUser);

            transactionLog.clear();
            System.out.println("COMMIT;");
            System.out.println("Transaction committed successfully.");
        } catch (Exception e) {
            System.out.println("Error committing transaction: " + e.getMessage());
        } finally {
            transactionActive = false;
        }
    }
    private void rollbackTransaction() {
        if (!transactionActive) {
            System.out.println("Error: No active transaction to rollback.");
            return;
        }

        storage.rollbackTransaction(loggedInUser);

        transactionLog.clear();
        System.out.println("ROLLBACK;");
        System.out.println("Transaction rolled back. All staged changes discarded.");
        transactionActive = false;
    }
    private boolean isTransactionActive() {
        return transactionActive;
    }

    private void stageQuery(String query) {
        if (transactionActive) {
            transactionLog.add(query);
            System.out.println("Query staged for transaction: " + query);
        } else {
            executeQuery(query);
        }
    }

    private boolean handleTransactionCommand(String query, String upperQuery) {
        Pattern beginPattern = Pattern.compile("BEGIN(\\s+TRANSACTION)?(\\s+(AS|LIKE|SET)\\s+(\\w+))?", Pattern.CASE_INSENSITIVE);
        Pattern commitPattern = Pattern.compile("COMMIT(\\s+(AS|LIKE|SET)\\s+(\\w+))?", Pattern.CASE_INSENSITIVE);
        Pattern rollbackPattern = Pattern.compile("ROLLBACK(\\s+(AS|LIKE|SET)\\s+(\\w+))?", Pattern.CASE_INSENSITIVE);

        Matcher beginMatcher = beginPattern.matcher(query);
        if (beginMatcher.matches()) {
            String user = beginMatcher.group(4);
            if (user != null) {
                beginTransactionAs(user);
            } else {
                beginTransaction();
            }
            return true;
        }

        Matcher commitMatcher = commitPattern.matcher(query);
        if (commitMatcher.matches()) {
            String user = commitMatcher.group(3);
            if (user != null) {
                commitTransactionAs(user);
            } else {
                commitTransaction();
            }
            return true;
        }

        Matcher rollbackMatcher = rollbackPattern.matcher(query);
        if (rollbackMatcher.matches()) {
            String user = rollbackMatcher.group(3);
            if (user != null) {
                rollbackTransactionAs(user);
            } else {
                rollbackTransaction();
            }
            return true;
        }

        return false;
    }

    private void beginTransactionAs(String user) {
        if (transactionActive) {
            System.out.println("Error: A transaction is already active.");
            return;
        }
        storage.beginTransaction(user);
        transactionActive = true;
        transactionLog.clear();
        System.out.println("BEGIN TRANSACTION AS " + user + ";");
        System.out.println("Transaction mode is activated for user: " + user);
    }
    
    private void commitTransactionAs(String user) {
        if (!transactionActive) {
            System.out.println("Error: No active transaction to commit.");
            return;
        }

        try {
            for (String query : transactionLog) {
                System.out.println("Applying query: " + query);
            }

            storage.commitTransaction(user);

            transactionLog.clear();
            System.out.println("COMMIT AS " + user + ";");
            System.out.println("Transaction committed successfully for user: " + user);
        } catch (Exception e) {
            System.out.println("Error committing transaction: " + e.getMessage());
        } finally {
            transactionActive = false;
        }
    }

    private void rollbackTransactionAs(String user) {
        if (!transactionActive) {
            System.out.println("Error: No active transaction to rollback.");
            return;
        }
        storage.rollbackTransaction(user);
        transactionLog.clear();
        System.out.println("ROLLBACK AS " + user + ";");
        System.out.println("Transaction rolled back for user: " + user + ". All staged changes discarded.");
        transactionActive = false;
    }
}
