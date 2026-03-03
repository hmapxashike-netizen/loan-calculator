import java.util.Scanner;

import loancalculator.StandardisedTermLoanConsole;

/**
 * Main menu for the Loan Management System.
 * Navigate to modules from here.
 */
public class Main {
    public static void main(String[] args) {
        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                System.out.println();
                System.out.println("=== Loan Management System ===");
                System.out.println();
                System.out.println("  1) Standardised Term Loan");
                System.out.println("  2) Exit");
                System.out.print("Enter choice (1-2): ");

                int choice = readChoice(scanner, 1, 2);
                if (choice == 2) {
                    System.out.println("Goodbye.");
                    break;
                }

                System.out.println();
                System.out.println("=== Standardised Term Loan ===");
                System.out.println();
                System.out.println("  1) Web interface (start HTTP server)");
                System.out.println("  2) Console calculator");
                System.out.println("  3) Back to main menu");
                System.out.print("Enter choice (1-3): ");

                int subChoice = readChoice(scanner, 1, 3);
                if (subChoice == 1) {
                    startWebServer();
                } else if (subChoice == 2) {
                    StandardisedTermLoanConsole.run(scanner);
                }
                // 3 = back, loop continues
            }
        }
    }

    private static void startWebServer() {
        System.out.println();
        System.out.println("Starting Standardised Term Loan HTTP server on port 8080...");
        System.out.println("Open the web interface at http://localhost:5173 (run: cd lms-frontend && npm run dev)");
        System.out.println("Press Ctrl+C to stop the server.");
        System.out.println();
        try {
            LoanServer.main(new String[0]);
        } catch (Exception e) {
            System.err.println("Failed to start server: " + e.getMessage());
        }
    }

    private static int readChoice(Scanner scanner, int min, int max) {
        while (true) {
            try {
                int v = Integer.parseInt(scanner.nextLine().trim());
                if (v >= min && v <= max) return v;
                System.out.printf("Enter %d-%d. Try again: ", min, max);
            } catch (NumberFormatException e) {
                System.out.print("Invalid choice. Try again: ");
            }
        }
    }
}
