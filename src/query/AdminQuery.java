package query;

import interfaces.IAdminQuery;
import storage.PersistentStorage;
import java.util.List;
import java.util.Scanner;

public class AdminQuery implements IAdminQuery {
    private final PersistentStorage storage;

    public AdminQuery() {
        this.storage = new PersistentStorage();
    }

    @Override
    public void queryLogs() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter your SQL-like query (e.g., SELECT * FROM logs WHERE user = 'admin'):");
        String query = scanner.nextLine().trim();

        if (!query.startsWith("SELECT * FROM logs")) {
            System.out.println("Invalid query format. Use: SELECT * FROM logs WHERE user = 'username'");
            return;
        }

        String filterUser = extractUsernameFromQuery(query);
        List<String[]> logs = storage.readLogs();
        boolean found = false;

        System.out.println("\n=== Authentication Logs ===");
        for (String[] log : logs) {
            String timestamp = log[0];
            String user = log[1];
            String status = log[2];

            if (filterUser.isEmpty() || user.equals(filterUser)) {
                System.out.printf("%s | %s | %s%n", timestamp, user, status);
                found = true;
            }
        }

        if (!found) {
            System.out.println("No logs found for user: " + filterUser);
        }
    }

    private String extractUsernameFromQuery(String query) {
        if (query.contains("WHERE user = '")) {
            return query.split("WHERE user = '")[1].replace("'", "").trim();
        }
        return "";
    }

    @Override
    public void adminMenu() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("\n=== Admin Panel ===");
            System.out.println("1. Query Logs");
            System.out.println("2. Exit");
            System.out.print("Choose an option: ");

            int choice = scanner.nextInt();
            scanner.nextLine();

            switch (choice) {
                case 1:
                    queryLogs();
                    break;
                case 2:
                    System.out.println("Exiting Admin Panel...");
                    return;
                default:
                    System.out.println("Invalid Choice. Try Again.");
            }
        }
    }
}
