package hvfhv.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Pages through the NYC Open Data (Socrata) HVFHV trip dataset in a deterministic order.
 * Paging by offset is only stable with a total order, so rows are sorted by request time
 * and then by Socrata's row id, the same order scripts/download_sample.sh uses.
 */
public final class SocrataClient implements TripSource {

    static final String ORDER = "request_datetime,:id";

    private final URI endpoint;
    private final String fromInclusive;
    private final Optional<String> untilExclusive;
    private final Optional<String> appToken;
    private final HttpClient http;
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * @param fromInclusive  first request_datetime to stream, e.g. 2022-01-02T00:00:00; must equal
     *                       the end of the historical load so the two never overlap
     * @param untilExclusive optional request_datetime to stop at
     */
    public SocrataClient(URI endpoint, String fromInclusive, Optional<String> untilExclusive, Optional<String> appToken) {
        this.endpoint = endpoint;
        this.fromInclusive = fromInclusive;
        this.untilExclusive = untilExclusive;
        this.appToken = appToken;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
    }

    URI pageUri(long offset, int limit) {
        String where = "request_datetime >= '" + fromInclusive + "'"
                + untilExclusive.map(u -> " AND request_datetime < '" + u + "'").orElse("");
        Map<String, String> params = new LinkedHashMap<>();
        params.put("$where", where);
        params.put("$order", ORDER);
        params.put("$limit", Integer.toString(limit));
        params.put("$offset", Long.toString(offset));
        String query = params.entrySet().stream()
                .map(e -> encode(e.getKey()) + "=" + encode(e.getValue()))
                .collect(Collectors.joining("&"));
        return URI.create(endpoint + "?" + query);
    }

    @Override
    public List<String> fetchPage(long offset, int limit) throws IOException, InterruptedException {
        HttpRequest.Builder request = HttpRequest.newBuilder(pageUri(offset, limit))
                .timeout(Duration.ofMinutes(2))
                .header("Accept", "application/json");
        appToken.ifPresent(token -> request.header("X-App-Token", token));

        HttpResponse<String> response = http.send(request.GET().build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("Socrata returned HTTP " + response.statusCode() + ": " + response.body());
        }
        return parsePage(response.body());
    }

    List<String> parsePage(String body) throws IOException {
        JsonNode rows = mapper.readTree(body);
        if (!rows.isArray()) {
            throw new IOException("Expected a JSON array from Socrata, got: " + body);
        }
        List<String> trips = new ArrayList<>(rows.size());
        for (JsonNode row : rows) {
            trips.add(mapper.writeValueAsString(row));
        }
        return trips;
    }

    private static String encode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
