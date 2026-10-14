package hvfhv.producer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TripProducerTest {

    @TempDir
    Path tmp;

    /** Serves `total` numbered trips in pages, like Socrata's offset paging. */
    private static TripSource pages(int total) {
        return (offset, limit) -> {
            List<String> page = new ArrayList<>();
            for (long i = offset; i < Math.min(total, offset + limit); i++) {
                page.add("{\"trip\":" + i + "}");
            }
            return page;
        };
    }

    private ProducerSettings settings(Optional<String> until) {
        Map<String, String> env = new HashMap<>(Map.of(
                "SPLIT_DATETIME", "2022-01-02T00:00:00",
                "PAGE_SIZE", "2",
                "OFFSET_FILE", tmp.resolve("offset").toString()));
        until.ifPresent(u -> env.put("STREAM_UNTIL", u));
        return ProducerSettings.fromEnv(env);
    }

    private static MockProducer<String, String> kafka() {
        return new MockProducer<>(true, new StringSerializer(), new StringSerializer());
    }

    @Test
    void offsetStoreRoundTripsAndDefaultsWhenMissing() {
        OffsetStore store = new OffsetStore(tmp.resolve("nested/offset"));
        assertEquals(7L, store.load(7L));
        store.save(42L);
        assertEquals(42L, store.load(7L));
        store.save(43L);
        assertEquals(43L, new OffsetStore(tmp.resolve("nested/offset")).load(0L));
    }

    @Test
    void publishesPagesInOrderAndPersistsTheOffset() {
        MockProducer<String, String> kafka = kafka();
        ProducerSettings s = settings(Optional.empty());
        TripProducer producer = new TripProducer(pages(3), kafka, new OffsetStore(s.offsetFile()), s);

        producer.run();
        producer.run();
        producer.run(); // nothing left: no-op

        assertEquals(List.of("{\"trip\":0}", "{\"trip\":1}", "{\"trip\":2}"),
                kafka.history().stream().map(r -> r.value()).toList());
        assertEquals(3L, new OffsetStore(s.offsetFile()).load(0L));
    }

    @Test
    void resumesFromTheSavedOffsetInsteadOfReplaying() {
        ProducerSettings s = settings(Optional.empty());
        new OffsetStore(s.offsetFile()).save(2L);
        MockProducer<String, String> kafka = kafka();

        new TripProducer(pages(3), kafka, new OffsetStore(s.offsetFile()), s).run();

        assertEquals(List.of("{\"trip\":2}"), kafka.history().stream().map(r -> r.value()).toList());
    }

    @Test
    void aFailedSendLeavesTheOffsetSoThePageIsRetried() {
        ProducerSettings s = settings(Optional.empty());
        MockProducer<String, String> kafka = kafka();
        kafka.sendException = new KafkaException("broker down");
        TripProducer producer = new TripProducer(pages(3), kafka, new OffsetStore(s.offsetFile()), s);

        producer.run(); // must not throw: that would cancel the scheduled task
        assertEquals(0L, new OffsetStore(s.offsetFile()).load(0L));

        kafka.sendException = null;
        producer.run();
        assertEquals(2L, new OffsetStore(s.offsetFile()).load(0L));
    }

    @Test
    @Timeout(5)
    void aBoundedStreamSignalsWhenExhausted() throws InterruptedException {
        ProducerSettings s = settings(Optional.of("2022-01-03T00:00:00"));
        TripProducer producer = new TripProducer(pages(1), kafka(), new OffsetStore(s.offsetFile()), s);
        producer.run();
        producer.run();
        producer.awaitExhausted();
    }

    @Test
    void socrataPagesUseAStableTotalOrderAndTheHistoricalCutOff() {
        SocrataClient client = new SocrataClient(URI.create("https://example.org/resource/x.json"),
                "2022-01-02T00:00:00", Optional.of("2022-01-03T00:00:00"), Optional.empty());
        String query = URLDecoder.decode(client.pageUri(500, 100).getRawQuery(), StandardCharsets.UTF_8);

        assertTrue(query.contains("$where=request_datetime >= '2022-01-02T00:00:00' AND request_datetime < '2022-01-03T00:00:00'"), query);
        assertTrue(query.contains("$order=request_datetime,:id"), query);
        assertTrue(query.contains("$limit=100") && query.contains("$offset=500"), query);
    }

    @Test
    void socrataRowsAreForwardedAsCompactJson() throws Exception {
        SocrataClient client = new SocrataClient(URI.create("https://example.org"), "x", Optional.empty(), Optional.empty());
        assertEquals(List.of("{\"a\":\"1\"}", "{\"b\":\"2\"}"), client.parsePage("[ {\"a\": \"1\"}, {\"b\": \"2\"} ]"));
        assertThrows(java.io.IOException.class, () -> client.parsePage("{\"error\": true}"));
    }

    @Test
    void settingsRequireTheSplitDatetime() {
        assertThrows(IllegalArgumentException.class, () -> ProducerSettings.fromEnv(Map.of()));
    }
}
