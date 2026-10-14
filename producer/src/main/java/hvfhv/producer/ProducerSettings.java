package hvfhv.producer;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/** Producer settings, read from environment variables (see README). */
public record ProducerSettings(
        String brokers,
        String topic,
        URI socrataEndpoint,
        String streamFrom,
        Optional<String> streamUntil,
        int pageSize,
        Duration pollInterval,
        Path offsetFile,
        Optional<String> appToken) {

    static final String DEFAULT_ENDPOINT = "https://data.cityofnewyork.us/resource/g6pj-fsah.json";

    public static ProducerSettings fromEnv(Map<String, String> env) {
        return new ProducerSettings(
                env.getOrDefault("KAFKA_BROKERS", "localhost:9092"),
                env.getOrDefault("KAFKA_TOPIC", "hvfhv-trips"),
                URI.create(env.getOrDefault("SOCRATA_ENDPOINT", DEFAULT_ENDPOINT)),
                required(env, "SPLIT_DATETIME"),
                optional(env, "STREAM_UNTIL"),
                Integer.parseInt(env.getOrDefault("PAGE_SIZE", "10000")),
                Duration.ofSeconds(Long.parseLong(env.getOrDefault("POLL_INTERVAL_SECONDS", "10"))),
                Path.of(env.getOrDefault("OFFSET_FILE", "data/producer/offset")),
                optional(env, "SOCRATA_APP_TOKEN"));
    }

    private static String required(Map<String, String> env, String name) {
        return optional(env, name).orElseThrow(() -> new IllegalArgumentException(
                name + " is required (the request_datetime where the historical load ends, e.g. 2022-01-02T00:00:00)"));
    }

    private static Optional<String> optional(Map<String, String> env, String name) {
        return Optional.ofNullable(env.get(name)).map(String::trim).filter(v -> !v.isEmpty());
    }
}
