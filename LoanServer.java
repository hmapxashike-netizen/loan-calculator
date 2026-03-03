import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import loancalculator.ScheduleRow;
import loancalculator.StandardisedTermLoan;
import loancalculator.StandardisedTermLoanRequest;
import loancalculator.StandardisedTermLoanResult;

/**
 * HTTP server exposing the Standardised Term Loan Calculator.
 * POST /calculate with JSON body returns loan summary and amortisation schedule.
 */
public class LoanServer {
    private static final int PORT = 8080;

    public static void main(String[] args) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/calculate", exchange -> {
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                sendCors(exchange);
                return;
            }
            handleCalculate(exchange);
        });
        server.createContext("/health", exchange -> {
            sendJson(exchange, 200, "{\"status\":\"ok\",\"message\":\"Standardised Term Loan engine ready\"}");
        });
        server.setExecutor(null);
        server.start();
        System.out.println("Standardised Term Loan HTTP server running at http://localhost:" + PORT);
    }

    private static void handleCalculate(HttpExchange exchange) {
        try {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, "{\"error\":\"Method not allowed\"}");
                return;
            }
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            StandardisedTermLoanRequest req = parseRequest(body);
            StandardisedTermLoanResult result = StandardisedTermLoan.compute(req);
            sendJson(exchange, 200, toJson(result));
        } catch (Exception e) {
            try {
                String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                sendJson(exchange, 400, "{\"error\":\"" + escapeJson(msg) + "\"}");
            } catch (Exception e2) {
                e.printStackTrace();
            }
        }
    }

    private static StandardisedTermLoanRequest parseRequest(String body) {
        String scheme = orDefault(extractString(body, "scheme", "TPC"), "TPC");
        String amountType = orDefault(extractString(body, "amountType", "amountRequired"), "amountRequired");
        double amount = extractDouble(body, "amount", 100.0);
        int tenorMonths = extractInt(body, "tenorMonths", 6);
        String disb = extractString(body, "disbursementDate", null);
        if (disb == null) disb = extractString(body, "startDate", null);
        String disbursement = orDefault(disb, LocalDate.now().toString());
        String firstRepay = orDefault(extractString(body, "firstRepaymentDate", null), LocalDate.now().plusMonths(1).toString());

        LocalDate disbursementDate = parseDate(disbursement.trim(), "Disbursement date");
        LocalDate firstRepaymentDate = parseDate(firstRepay.trim(), "Date of first repayment");

        return StandardisedTermLoanRequest.of(scheme, amountType, amount, tenorMonths, disbursementDate, firstRepaymentDate);
    }

    private static LocalDate parseDate(String s, String fieldName) {
        try {
            return LocalDate.parse(s);
        } catch (Exception e) {
            throw new IllegalArgumentException(fieldName + " must be YYYY-MM-DD (e.g. 2025-03-15)");
        }
    }

    private static String orDefault(String s, String def) {
        return (s != null && !s.trim().isEmpty()) ? s.trim() : def;
    }

    private static String extractString(String json, String key, String def) {
        if (json == null || json.isEmpty()) return def;
        Pattern p = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*\"([^\"]*)\"");
        Matcher m = p.matcher(json);
        return m.find() ? m.group(1) : def;
    }

    private static double extractDouble(String json, String key, double def) {
        Pattern p = Pattern.compile("\"" + key + "\"\\s*:\\s*([\\d.]+)");
        Matcher m = p.matcher(json);
        return m.find() ? Double.parseDouble(m.group(1)) : def;
    }

    private static int extractInt(String json, String key, int def) {
        Pattern p = Pattern.compile("\"" + key + "\"\\s*:\\s*(\\d+)");
        Matcher m = p.matcher(json);
        return m.find() ? Integer.parseInt(m.group(1)) : def;
    }

    private static String toJson(StandardisedTermLoanResult r) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"facilityAmount\":").append(r.facilityAmount).append(",");
        sb.append("\"amountRequired\":").append(r.amountRequired).append(",");
        sb.append("\"scheme\":\"").append(escapeJson(r.scheme)).append("\",");
        sb.append("\"adminPercent\":").append(r.adminPercent).append(",");
        sb.append("\"effectiveInterestPercent\":").append(r.effectiveInterestPercent).append(",");
        sb.append("\"tenorMonths\":").append(r.tenorMonths).append(",");
        sb.append("\"disbursementDate\":\"").append(r.disbursementDate).append("\",");
        sb.append("\"firstRepaymentDate\":\"").append(r.firstRepaymentDate).append("\",");
        sb.append("\"endDate\":\"").append(r.endDate).append("\",");
        sb.append("\"monthlyInstallment\":").append(r.monthlyInstallment).append(",");
        sb.append("\"schedule\":[");
        for (int i = 0; i < r.schedule.size(); i++) {
            ScheduleRow row = r.schedule.get(i);
            if (i > 0) sb.append(",");
            sb.append("{\"period\":").append(row.period)
              .append(",\"dueDate\":\"").append(row.dueDate).append("\"")
              .append(",\"payment\":").append(row.payment)
              .append(",\"interest\":").append(row.interest)
              .append(",\"principal\":").append(row.principal)
              .append(",\"principalBalance\":").append(row.principalBalance)
              .append(",\"outstandingBalance\":").append(row.outstandingBalance).append("}");
        }
        sb.append("]}");
        return sb.toString();
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    private static void sendCors(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "POST, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
        exchange.sendResponseHeaders(204, -1);
        exchange.close();
    }

    private static void sendJson(HttpExchange exchange, int code, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
