package loancalculator;

import java.util.List;

/**
 * Result of a Standardised Term Loan calculation.
 * Can be used from within the module or from external callers.
 */
public class StandardisedTermLoanResult {
    public double facilityAmount;
    public double amountRequired;
    public String scheme;
    public double adminPercent;
    public double effectiveInterestPercent;
    public int tenorMonths;
    public String disbursementDate;
    public String firstRepaymentDate;
    public String endDate;
    public double monthlyInstallment;
    public List<ScheduleRow> schedule;
}
