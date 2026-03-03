package loancalculator;

import java.time.LocalDate;

/**
 * Request parameters for the Standardised Term Loan calculator.
 * Can be used from within the module or from external callers.
 */
public class StandardisedTermLoanRequest {
    public String scheme = "TPC";
    public String amountType = "amountRequired";
    public double amount;
    public int tenorMonths;
    public LocalDate disbursementDate;
    public LocalDate firstRepaymentDate;

    public static StandardisedTermLoanRequest of(String scheme, String amountType, double amount,
            int tenorMonths, LocalDate disbursementDate, LocalDate firstRepaymentDate) {
        StandardisedTermLoanRequest r = new StandardisedTermLoanRequest();
        r.scheme = scheme != null ? scheme : "TPC";
        r.amountType = amountType != null ? amountType : "amountRequired";
        r.amount = amount;
        r.tenorMonths = tenorMonths;
        r.disbursementDate = disbursementDate;
        r.firstRepaymentDate = firstRepaymentDate;
        return r;
    }
}
