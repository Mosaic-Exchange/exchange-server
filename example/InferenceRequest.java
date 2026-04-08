import java.io.Serializable;

/**
 * Request payload for {@link InferenceService}.
 *
 * @param prompt          the user's prompt text
 * @param model           LLM model name (e.g. "llama3.2"); {@code null} uses the service default
 * @param effort          generation effort hint (e.g. "low", "medium", "high"); {@code null} uses default
 * @param maxOutputTokens maximum tokens to generate; {@code 0} means no limit
 * @param temperature     sampling temperature; {@code 0} means use model default
 */
public record InferenceRequest(
        String prompt,
        String model,
        String effort,
        int maxOutputTokens,
        double temperature
) implements Serializable {

    public InferenceRequest(String prompt) {
        this(prompt, null, null, 0, 0);
    }

    public InferenceRequest(String prompt, String model) {
        this(prompt, model, null, 0, 0);
    }
}
