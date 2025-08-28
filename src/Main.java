package src;

import authentication.Authentication;
import query.AdminQuery;
import query.QueryProcessor;

import java.util.Scanner;


public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        Authentication auth = new Authentication();
        AdminQuery adminQuery = new AdminQuery();

        while (true) {
            System.out.println("\n====== Lightweight DBMS ======");
            System.out.println("1. Register");
            System.out.println("2. Login");
            System.out.println("3. Recover/Reset Password");
            System.out.println("4. Exit");
            System.out.print("Enter choice: ");

            int choice = getUserChoice(scanner);
            switch (choice) {
                case 1:
                    auth.register();
                    break;
                case 2:
                    String loggedInUser = auth.login();
                    if (loggedInUser != null) {
                        QueryProcessor queryProcessor = new QueryProcessor(loggedInUser);
                        databaseOperations(queryProcessor, adminQuery, scanner);
                    }
                    break;
                case 3:
                    passwordRecoveryMenu(auth, scanner);
                    break;
                case 4:
                    System.out.println("Exiting Lightweight DBMS. Goodbye!");
                    System.exit(0);
                    break;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }
        }
    }

    private static void passwordRecoveryMenu(Authentication auth, Scanner scanner) {
        while (true) {
            System.out.println("\n====== Password Recovery ======");
            System.out.println("1. Generate Reset Token");
            System.out.println("2. Reset Password");
            System.out.println("3. Go Back");
            System.out.print("Enter choice: ");

            int option = getUserChoice(scanner);
            switch (option) {
                case 1:
                    auth.recoverPassword();
                    break;
                case 2:
                    auth.resetPassword();
                    break;
                case 3:
                    return;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }
        }
    }

    private static void databaseOperations(QueryProcessor queryProcessor, AdminQuery adminQuery, Scanner scanner) {
        boolean databaseSelected = false;

        while (true) {
            System.out.println("\n====== Database Operations ======");
            System.out.println("1. Execute SQL Query");
            System.out.println("2. Admin Log Query");
            System.out.println("3. Logout");
            System.out.print("Enter choice: ");

            int choice = getUserChoice(scanner);
            switch (choice) {
                case 1:
                    System.out.println("Enter SQL-like Query (End with ';'):");

                    StringBuilder queryBuilder = new StringBuilder();
                    while (true) {
                        String line = scanner.nextLine().trim();
                        queryBuilder.append(line).append(" ");
                        if (line.endsWith(";")) {
                            break;
                        }
                    }

                    String query = queryBuilder.toString().replaceAll("\\s+", " ").trim();

                    if (!query.endsWith(";")) {
                        System.out.println("Error: SQL queries must end with a semicolon (;).");
                        continue;
                    }

                    String upperQuery = query.toUpperCase();

                    if (upperQuery.startsWith("USE ")) {
                        if (databaseSelected) {
                            System.out.println("Error: Only one database can be used. Switching databases is not allowed.");
                            continue;
                        }
                        databaseSelected = true;
                    }

                    queryProcessor.executeQuery(query);
                    break;

                case 2:
                    adminQuery.adminMenu();
                    break;

                case 3:
                    System.out.println("Logging out...");
                    return;

                default:
                    System.out.println("Invalid choice. Please try again.");
            }
        }
    }

    private static int getUserChoice(Scanner scanner) {
        while (true) {
            try {
                String input = scanner.nextLine().trim();
                if (input.matches(".*\\s+.*")) {
                    System.out.println("Invalid input. Please enter a valid number.");
                    continue;
                }

                return Integer.parseInt(input);
            } catch (NumberFormatException e) {
                System.out.println("Invalid input. Please enter a valid number.");
            }
        }
    }
}
