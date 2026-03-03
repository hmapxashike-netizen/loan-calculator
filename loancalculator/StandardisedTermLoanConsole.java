package loancalculator;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Scanner;

/**
 * Console UI for the Standardised Term Loan calculator.
 * Uses the StandardisedTermLoan engine.
 */
public class StandardisedTermLoanConsole {
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ISO_LOCAL_DATE;

    public static void run(Scanner scanner) {
        System.out.println();
        System.out.println("=== Standardised Term Loan (Console) ===");

        System.out.println();
        System.out.println("Select Scheme:");
        System.out.println("  1) TPC  (Interest 7% per month, Admin 5%)");
        System.out.println("  2) SSB  (Interest 7% per month, Admin 7%)");
        System.out.print("Enter choice (1-2): ");
        int schemeChoice = readInt(scanner, 1, 2);
        String scheme = schemeChoice == 1 ? "TPC" : "SSB";

        System.out.println();
        System.out.println("Do you know:");
        System.out.println("  1) Principal Amount (total facility / debt)");
        System.out.println("  2) Amount Required (net proceeds to client)");
        System.out.print("Enter choice (1-2): ");
        int amountChoice = readInt(scanner, 1, 2);
        String amountType = amountChoice == 1 ? "principal" : "amountRequired";

        double amount;
        if (amountType.equals("principal")) {
            System.out.print("Enter Principal Amount: ");
            amount = readPositiveDouble(scanner);
        } else {
            System.out.print("Enter Amount Required (net proceeds): ");
            amount = readPositiveDouble(scanner);
        }

        System.out.print("Enter Tenor (months): ");
        int tenorMonths = readPositiveInt(scanner);

        System.out.print("Enter Disbursement Date (YYYY-MM-DD): ");
        LocalDate disbursementDate = readDate(scanner);

        System.out.print("Enter Date of First Repayment (YYYY-MM-DD): ");
        LocalDate firstRepaymentDate = readDate(scanner);

        try {
            StandardisedTermLoanRequest req = StandardisedTermLoanRequest.of(
                    scheme, amountType, amount, tenorMonths, disbursementDate, firstRepaymentDate);
            StandardisedTermLoanResult result = StandardisedTermLoan.compute(req);

            System.out.println();
            System.out.println("--- Result ---");
            System.out.printf("Amount Required:         %.2f%n", result.amountRequired);
            System.out.printf("Facility Amount:         %.2f%n", result.facilityAmount);
            System.out.printf("Scheme:                   %s%n", result.scheme);
            System.out.printf("Administration Fees (%%):  %.2f%%%n", result.adminPercent);
            System.out.printf("Effective Interest (%%/month): %.2f%%%n", result.effectiveInterestPercent);
            System.out.printf("Tenor (months):           %d%n", result.tenorMonths);
            System.out.printf("Disbursement Date:       %s%n", result.disbursementDate);
            System.out.printf("First Repayment Date:    %s%n", result.firstRepaymentDate);
            System.out.printf("End Date:                %s%n", result.endDate);
            System.out.printf("Monthly Instalment:      %.2f%n", result.monthlyInstallment);

            System.out.println();
            System.out.println("=== Amortisation Schedule ===");
            System.out.printf("%-6s %-12s %14s %14s %14s %14s %14s%n",
                    "Period", "Due Date", "Payment", "Interest", "Principal", "Principal Bal", "Outstanding");
            System.out.println("---------------------------------------------------------------------------------------------");
            for (ScheduleRow row : result.schedule) {
                System.out.printf("%-6d %-12s %14.2f %14.2f %14.2f %14.2f %14.2f%n",
                        row.period, row.dueDate, row.payment, row.interest, row.principal,
                        row.principalBalance, row.outstandingBalance);
            }
        } catch (IllegalArgumentException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private static LocalDate readDate(Scanner scanner) {
        while (true) {
            String line = scanner.nextLine().trim();
            try {
                return LocalDate.parse(line, DATE_FMT);
            } catch (DateTimeParseException e) {
                System.out.print("Invalid date. Use YYYY-MM-DD (e.g. 2025-03-15). Try again: ");
            }
        }
    }

    private static double readPositiveDouble(Scanner scanner) {
        while (true) {
            try {
                double v = Double.parseDouble(scanner.nextLine().trim());
                if (v > 0) return v;
                System.out.print("Value must be > 0. Try again: ");
            } catch (NumberFormatException e) {
                System.out.print("Invalid number. Try again: ");
            }
        }
    }

    private static int readPositiveInt(Scanner scanner) {
        while (true) {
            try {
                int v = Integer.parseInt(scanner.nextLine().trim());
                if (v > 0) return v;
                System.out.print("Value must be > 0. Try again: ");
            } catch (NumberFormatException e) {
                System.out.print("Invalid integer. Try again: ");
            }
        }
    }

    private static int readInt(Scanner scanner, int min, int max) {
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
