package org.rumor.app;

import org.rumor.service.RStreamingService;
import org.rumor.service.ServiceResponse;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

/**
 * LLM chat inference service using the Ollama HTTP API.
 *
 * <p>Request format (UTF-8): the user's prompt text.
 * <p>Response: streamed text tokens as they arrive from the LLM.
 *
 * <p>Requires an Ollama server running (e.g. {@code ollama serve}).
 * Endpoint and model are configured at construction time.
 */
public class InferenceService extends RStreamingService {

    private final String endpoint;
    private final String model;
    private final HttpClient httpClient;

    public InferenceService() {
        this("http://localhost:11434", "llama3.2");
    }

    public InferenceService(String endpoint, String model) {
        this.endpoint = endpoint;
        this.model = model;
        this.httpClient = HttpClient.newHttpClient();
    }

    @Override
    public void serve(byte[] request, ServiceResponse response) {
        String prompt = new String(request, StandardCharsets.UTF_8);

        String jsonBody = "{\"model\":\"" + escapeJson(model)
                + "\",\"prompt\":\"" + escapeJson(prompt)
                + "\",\"stream\":true}";

        HttpRequest httpReq = HttpRequest.newBuilder()
                .uri(URI.create(endpoint + "/api/generate"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();

        try {
            HttpResponse<Stream<String>> httpResp = httpClient.send(
                    httpReq, HttpResponse.BodyHandlers.ofLines());

            httpResp.body().forEach(line -> {
                if (line.isBlank()) return;

                String token = extractField(line, "response");
                if (token != null && !token.isEmpty()) {
                    response.write(token.getBytes(StandardCharsets.UTF_8));
                }
            });

            response.close();
        } catch (Exception e) {
            response.fail(e.getMessage().getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Minimal JSON string field extractor to avoid a library dependency.
     */
    private static String extractField(String json, String field) {
        String key = "\"" + field + "\":\"";
        int start = json.indexOf(key);
        if (start < 0) return null;
        start += key.length();

        StringBuilder sb = new StringBuilder();
        for (int i = start; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '\\' && i + 1 < json.length()) {
                char next = json.charAt(i + 1);
                switch (next) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case 'n' -> sb.append('\n');
                    case 't' -> sb.append('\t');
                    case 'r' -> sb.append('\r');
                    default -> { sb.append('\\'); sb.append(next); }
                }
                i++;
            } else if (c == '"') {
                break;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
