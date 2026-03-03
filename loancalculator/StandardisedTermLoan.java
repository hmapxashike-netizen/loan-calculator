package loancalculator;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Standardised Term Loan calculator engine.
 * Callable from within and outside the module for loan computation.
 */
public class StandardisedTermLoan {
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ISO_LOCAL_DATE;

    /**
     * Compute loan summary and amortisation schedule.
     *
     * @param request the loan parameters
     * @return the result with facility amount, instalment, schedule, etc.
     * @throws IllegalArgumentException if parameters are invalid
     */
    public static StandardisedTermLoanResult compute(StandardisedTermLoanRequest request) {
        double adminPercent;
        double interestPercent;
        switch (request.scheme.toUpperCase()) {
            case "TPC":
                adminPercent = 5.0;
                interestPercent = 7.0;
                break;
            case "SSB":
                adminPercent = 7.0;
                interestPercent = 7.0;
                break;
            default:
                adminPercent = 5.0;
                interestPercent = 7.0;
        }
        double adminDecimal = adminPercent / 100.0;

        if (adminDecimal >= 1.0) {
            throw new IllegalArgumentException("Administration fee must be less than 100%");
        }

        double principal;
        double amountRequested;
        if ("principal".equalsIgnoreCase(request.amountType)) {
            principal = request.amount;
            amountRequested = principal * (1.0 - adminDecimal);
        } else {
            amountRequested = request.amount;
            principal = amountRequested / (1.0 - adminDecimal);
        }
        if (principal <= 0 || Double.isNaN(principal) || Double.isInfinite(principal)) {
            throw new IllegalArgumentException("Invalid amount or administration fee");
        }

        LocalDate disbursementDate = request.disbursementDate;
        LocalDate firstRepaymentDate = request.firstRepaymentDate;
        if (disbursementDate == null) {
            throw new IllegalArgumentException("Disbursement date is required");
        }
        if (firstRepaymentDate == null) {
            throw new IllegalArgumentException("Date of first repayment is required");
        }
        if (firstRepaymentDate.isBefore(disbursementDate)) {
            throw new IllegalArgumentException("Date of first repayment must be on or after disbursement date");
        }

        long monthsToFirstRepayment = ChronoUnit.MONTHS.between(disbursementDate, firstRepaymentDate);
        int monthsBeyond = (int) Math.max(0, monthsToFirstRepayment - 1);
        double marginalPercent = monthsBeyond * 0.5;
        double effectiveInterestPercent = interestPercent + marginalPercent;
        double monthlyRate = effectiveInterestPercent / 100.0;

        double monthlyInstallment = pmt(monthlyRate, request.tenorMonths, principal);
        LocalDate endDate = firstRepaymentDate.plusMonths(request.tenorMonths - 1);

        List<ScheduleRow> schedule = buildSchedule(disbursementDate, firstRepaymentDate,
                request.tenorMonths, principal, monthlyRate, monthlyInstallment);

        StandardisedTermLoanResult r = new StandardisedTermLoanResult();
        r.facilityAmount = principal;
        r.amountRequired = amountRequested;
        r.scheme = request.scheme.toUpperCase();
        r.adminPercent = adminPercent;
        r.effectiveInterestPercent = effectiveInterestPercent;
        r.tenorMonths = request.tenorMonths;
        r.disbursementDate = disbursementDate.format(DATE_FMT);
        r.firstRepaymentDate = firstRepaymentDate.format(DATE_FMT);
        r.endDate = endDate.format(DATE_FMT);
        r.monthlyInstallment = monthlyInstallment;
        r.schedule = schedule;
        return r;
    }

    private static List<ScheduleRow> buildSchedule(LocalDate disbursementDate, LocalDate firstRepaymentDate,
            int tenorMonths, double principal, double monthlyRate, double monthlyInstallment) {
        List<ScheduleRow> rows = new ArrayList<>();

        ScheduleRow row0 = new ScheduleRow();
        row0.period = 0;
        row0.dueDate = disbursementDate.format(DATE_FMT);
        row0.payment = 0.0;
        row0.interest = 0.0;
        row0.principal = 0.0;
        row0.principalBalance = round2(principal);
        row0.outstandingBalance = round2(principal);
        rows.add(row0);

        double principalBalance = principal;
        for (int period = 1; period <= tenorMonths; period++) {
            LocalDate dueDate = firstRepaymentDate.plusMonths(period - 1);
            double interest = principalBalance * monthlyRate;
            double principalPortion;
            double payment;
            if (period == tenorMonths) {
                principalPortion = principalBalance;
                payment = interest + principalPortion;
            } else {
                principalPortion = monthlyInstallment - interest;
                payment = monthlyInstallment;
            }
            principalBalance -= principalPortion;
            ScheduleRow row = new ScheduleRow();
            row.period = period;
            row.dueDate = dueDate.format(DATE_FMT);
            row.payment = round2(payment);
            row.interest = round2(interest);
            row.principal = round2(principalPortion);
            row.principalBalance = round2(principalBalance);
            row.outstandingBalance = round2(principalBalance);
            rows.add(row);
        }
        return rows;
    }

    private static double pmt(double rate, int nper, double pv) {
        if (nper <= 0) throw new IllegalArgumentException("Number of periods must be positive");
        if (rate == 0.0) return pv / nper;
        double factor = Math.pow(1.0 + rate, -nper);
        return (rate * pv) / (1.0 - factor);
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
