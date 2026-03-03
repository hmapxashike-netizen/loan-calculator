package loancalculator;

/**
 * A single row in the amortisation schedule.
 */
public class ScheduleRow {
    public int period;
    public String dueDate;
    public double payment;
    public double interest;
    public double principal;
    public double principalBalance;
    public double outstandingBalance;
}
