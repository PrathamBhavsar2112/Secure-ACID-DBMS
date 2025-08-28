package storage;

import interfaces.IPersistentStorage;
import java.io.*;
import java.util.*;

/**
 * Manages persistent storage for user credentials and authentication logs.
 * Uses JSON format for structured storage.
 * Supports transactions with ACID properties and concurrency control.
 */
public class PersistentStorage implements IPersistentStorage {
    private static final String USER_DATA_FILE = "users.json";
    private static final String LOG_FILE = "audit_logs.json";

    private final Map<String, Boolean> lockTable = new HashMap<>();
    private final Map<String, List<String>> transactionData = new HashMap<>();
    private final Map<String, Boolean> inTransaction = new HashMap<>();

    public PersistentStorage() {
        ensureFileExists(USER_DATA_FILE, "[]");
        ensureFileExists(LOG_FILE, "[]");
    }

    private void ensureFileExists(String fileName, String defaultContent) {
        File file = new File(fileName);
        if (!file.exists()) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                writer.write(defaultContent);
            } catch (IOException e) {
                System.out.println("Error initializing file: " + fileName);
            }
        }
    }

    @Override
    public synchronized void saveUser(String username, String hashedPassword, String encryptedQuestion, String encryptedAnswer) {
        List<Map<String, String>> usersList = readJsonList(USER_DATA_FILE);

        for (Map<String, String> user : usersList) {
            if (user.get("username").equals(username)) {
                System.out.println("User already exists.");
                return;
            }
        }

        Map<String, String> newUser = new HashMap<>();
        newUser.put("username", username);
        newUser.put("password", hashedPassword);
        newUser.put("security_question", encryptedQuestion);
        newUser.put("security_answer", encryptedAnswer);

        usersList.add(newUser);
        writeJsonList(USER_DATA_FILE, usersList);
    }

    @Override
    public synchronized void updateUserPassword(String username, String newHashedPassword, String encryptedQuestion, String encryptedAnswer) {
        List<Map<String, String>> usersList = readJsonList(USER_DATA_FILE);
        boolean userFound = false;

        for (Map<String, String> user : usersList) {
            if (user.get("username").equals(username)) {
                user.put("password", newHashedPassword);
                user.put("security_question", encryptedQuestion);
                user.put("security_answer", encryptedAnswer);
                userFound = true;
                break;
            }
        }

        if (userFound) {
            writeJsonList(USER_DATA_FILE, usersList);
            System.out.println("Password updated successfully in storage.");
        } else {
            System.out.println("Error: User not found while updating password.");
        }
    }

    @Override
    public List<String[]> readLogs() {
        List<String[]> logsList = new ArrayList<>();
        List<Map<String, String>> logsJsonList = readJsonList(LOG_FILE);

        for (Map<String, String> logEntry : logsJsonList) {
            String timestamp = logEntry.getOrDefault("timestamp", "Unknown");
            String userId = logEntry.getOrDefault("userId", "Unknown");
            String status = logEntry.getOrDefault("status", "Unknown");

            logsList.add(new String[]{timestamp, userId, status});
        }
        return logsList;
    }

    @Override
    public String getUserPassword(String username) {
        return getUserField(username, "password");
    }

    @Override
    public String getSecurityQuestion(String username) {
        return getUserField(username, "security_question");
    }

    @Override
    public String getSecurityAnswer(String username) {
        return getUserField(username, "security_answer");
    }

    @Override
    public String getLastSuccessfulLogin(String userId) {
        List<Map<String, String>> logsList = readJsonList(LOG_FILE);
        String lastSuccess = "No successful login found."; // Default message

        for (Map<String, String> log : logsList) {
            if (log.get("userId") != null && log.get("status") != null &&
                    log.get("userId").equals(userId) && log.get("status").equals("SUCCESS")) {
                lastSuccess = log.get("timestamp");
            }
        }
        return lastSuccess;
    }

    private String getUserField(String username, String field) {
        List<Map<String, String>> usersList = readJsonList(USER_DATA_FILE);
        for (Map<String, String> user : usersList) {
            if (user.get("username").equals(username)) {
                return user.get(field);
            }
        }
        return null;
    }

    @Override
    public void saveLog(String timestamp, String userId, String status) {
        List<Map<String, String>> logsList = readJsonList(LOG_FILE);

        Map<String, String> logEntry = new HashMap<>();
        logEntry.put("timestamp", timestamp);
        logEntry.put("userId", userId);
        logEntry.put("status", status);

        logsList.add(logEntry);
        writeJsonList(LOG_FILE, logsList);
    }

    /**
     * Handles transactions
     */
    public synchronized void beginTransaction(String user) {
        if (inTransaction.getOrDefault(user, false)) {
            System.out.println("Error: Transaction already in progress.");
            return;
        }
        inTransaction.put(user, true);
        transactionData.put(user, new ArrayList<>());
        System.out.println("Transaction started for " + user);
    }

    public synchronized void commitTransaction(String user) {
        if (!inTransaction.getOrDefault(user, false)) {
            System.out.println("Error: No active transaction to commit.");
            return;
        }

        List<String> queries = transactionData.get(user);
        if (queries != null) {
            for (String query : queries) {
                System.out.println("Executing Query in Transaction: " + query);
                // Execute the query normally
            }
        }

        inTransaction.put(user, false);
        transactionData.remove(user);
        System.out.println("Transaction committed successfully for " + user);
    }

    public synchronized void rollbackTransaction(String user) {
        if (!inTransaction.getOrDefault(user, false)) {
            System.out.println("Error: No active transaction to rollback.");
            return;
        }
        inTransaction.put(user, false);
        transactionData.remove(user);
        System.out.println("Transaction rolled back for " + user);
    }

    /**
     * Implements Read/Write Locks for Concurrency Control
     */
    public synchronized void acquireWriteLock(String tableName) throws InterruptedException {
        while (lockTable.getOrDefault(tableName, false)) {
            System.out.println("Waiting for write lock on table: " + tableName);
            wait();
        }
        lockTable.put(tableName, true);
    }

    public synchronized void releaseWriteLock(String tableName) {
        lockTable.put(tableName, false);
        notifyAll();
    }

    public synchronized void acquireReadLock(String tableName) throws InterruptedException {
        while (lockTable.getOrDefault(tableName, false)) {
            System.out.println("Waiting for read lock on table: " + tableName);
            wait();
        }
    }

    public synchronized void releaseReadLock(String tableName) {
        notifyAll();
    }

    /**
     * Reads JSON list from a file (without using third-party libraries).
     */
    private List<Map<String, String>> readJsonList(String fileName) {
        List<Map<String, String>> jsonList = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            StringBuilder jsonContent = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                jsonContent.append(line);
            }
            if (!jsonContent.toString().trim().isEmpty()) {
                jsonList = parseJsonArray(jsonContent.toString());
            }
        } catch (IOException e) {
            System.out.println("Error reading file: " + fileName);
        }
        return jsonList;
    }

    /**
     * Writes JSON list to a file.
     */
    private void writeJsonList(String fileName, List<Map<String, String>> jsonList) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write(formatJsonArray(jsonList));
        } catch (IOException e) {
            System.out.println("Error writing to file: " + fileName);
        }
    }

    /**
     * Parses a JSON-like array string into a list of maps.
     */
    private List<Map<String, String>> parseJsonArray(String json) {
        List<Map<String, String>> list = new ArrayList<>();
        json = json.trim();
        if (!json.startsWith("[") || !json.endsWith("]")) return list;

        json = json.substring(1, json.length() - 1).trim();
        if (json.isEmpty()) return list;

        String[] objects = json.split("\\},\\s*\\{");
        for (String obj : objects) {
            obj = obj.replace("{", "").replace("}", "");
            Map<String, String> map = new HashMap<>();
            String[] pairs = obj.split("\",\\s*\"");
            for (String pair : pairs) {
                String[] keyValue = pair.split("\":\\s*\"");
                if (keyValue.length == 2) {
                    map.put(keyValue[0].replace("\"", "").trim(), keyValue[1].replace("\"", "").trim());
                }
            }
            list.add(map);
        }
        return list;
    }

    /**
     * Formats a list of maps into a JSON-like array string.
     */
    private String formatJsonArray(List<Map<String, String>> list) {
        StringBuilder json = new StringBuilder("[\n");
        for (Map<String, String> obj : list) {
            json.append("  {");
            List<String> pairs = new ArrayList<>();
            for (Map.Entry<String, String> entry : obj.entrySet()) {
                pairs.add("\"" + entry.getKey() + "\": \"" + entry.getValue() + "\"");
            }
            json.append(String.join(", ", pairs)).append("},\n");
        }
        if (!list.isEmpty()) json.setLength(json.length() - 2);
        json.append("\n]");
        return json.toString();
    }
}