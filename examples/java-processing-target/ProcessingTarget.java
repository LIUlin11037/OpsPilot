import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProcessingTarget {
    private static final String PATH = "/processing";
    private static final List<Attempt> ATTEMPTS = List.of(
        new Attempt("order-msg-101", "succeeded", "2026-11-19T02:59:00Z"),
        new Attempt("order-msg-101", "failed", "2026-11-19T03:00:00Z"),
        new Attempt("order-msg-102", "succeeded", "2026-11-19T03:01:00Z")
    );

    private record Attempt(String messageId, String result, String attemptedAt) {
        String toJson(String queue, String vhost) {
            return """
                {"queue":%s,"vhost":%s,"message_id":%s,"result":%s,"attempted_at":%s}
                """.formatted(quote(queue), quote(vhost), quote(messageId),
                              quote(result), quote(attemptedAt));
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3 || args[1].isEmpty() || args[2].isEmpty()) {
            throw new IllegalArgumentException("Usage: ProcessingTarget.java PORT QUEUE VHOST");
        }

        int port = Integer.parseInt(args[0]);
        String queue = args[1];
        String vhost = args[2];
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.createContext(PATH, exchange -> handle(exchange, queue, vhost));
        server.start();
        System.out.println("http://127.0.0.1:" + server.getAddress().getPort() + PATH);
    }

    private static void handle(HttpExchange exchange, String queue, String vhost)
        throws IOException {
        try (exchange) {
            if (!PATH.equals(exchange.getRequestURI().getRawPath())) {
                sendJson(exchange, 404, "[]");
                return;
            }
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "GET");
                sendJson(exchange, 405, "{\"error\":\"Use GET\"}");
                return;
            }

            Map<String, String> query;
            try {
                query = parseQuery(exchange.getRequestURI().getRawQuery());
            } catch (IllegalArgumentException error) {
                sendJson(exchange, 400, "{\"error\":\"Invalid query parameters\"}");
                return;
            }

            String requestedQueue = query.getOrDefault("queue", "");
            String requestedVhost = query.getOrDefault("vhost", "");
            String messageId = query.get("message_id");
            if (requestedQueue.isEmpty() || requestedVhost.isEmpty()
                || (messageId != null && messageId.isBlank())) {
                sendJson(exchange, 400, "{\"error\":\"Provide queue and vhost, with an optional message_id\"}");
                return;
            }
            if (!queue.equals(requestedQueue) || !vhost.equals(requestedVhost)) {
                sendJson(exchange, 404, "[]");
                return;
            }

            List<Attempt> matches = ATTEMPTS.stream()
                .filter(attempt -> messageId == null || messageId.equals(attempt.messageId()))
                .toList();
            String body = matches.stream()
                .map(attempt -> attempt.toJson(queue, vhost))
                .collect(Collectors.joining(",", "[", "]"));
            sendJson(exchange, 200, body);
        }
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> query = new HashMap<>();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return query;
        }
        for (String pair : rawQuery.split("&", -1)) {
            String[] parts = pair.split("=", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Expected a query key and value");
            }
            String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
            if (query.putIfAbsent(key, value) != null) {
                throw new IllegalArgumentException("Duplicate query key");
            }
        }
        return query;
    }

    private static void sendJson(HttpExchange exchange, int status, String body)
        throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    private static String quote(String value) {
        StringBuilder output = new StringBuilder("\"");
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (character == '"' || character == '\\') {
                output.append('\\').append(character);
            } else if (character < ' ') {
                output.append("\\u%04x".formatted((int) character));
            } else {
                output.append(character);
            }
        }
        return output.append('"').toString();
    }
}
