package hvfhv.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Simulates a real-time feed: polls the next page of trips after the historical cut-off
 * and publishes each trip as one JSON message.
 *
 * The saved offset only advances after Kafka has acknowledged the whole page, so a crash
 * can re-send a page (at-least-once) but never skip one. Re-sent trips are removed again
 * by the batch layer's deduplication of the master dataset.
 *
 * Usage: SPLIT_DATETIME=2022-01-02T00:00:00 java -jar hvfhv-producer.jar [brokers]
 */
public final class TripProducer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(TripProducer.class);

    private final TripSource source;
    private final Producer<String, String> producer;
    private final OffsetStore offsets;
    private final ProducerSettings settings;
    private final CountDownLatch exhausted = new CountDownLatch(1);
    private long offset;

    TripProducer(TripSource source, Producer<String, String> producer, OffsetStore offsets, ProducerSettings settings) {
        this.source = source;
        this.producer = producer;
        this.offsets = offsets;
        this.settings = settings;
        this.offset = offsets.load(0L);
    }

    /** Publishes one page. Never throws: an exception would cancel the scheduled task. */
    @Override
    public void run() {
        try {
            List<String> trips = source.fetchPage(offset, settings.pageSize());
            if (trips.isEmpty()) {
                if (settings.streamUntil().isPresent()) {
                    log.info("Reached STREAM_UNTIL={} after {} trips", settings.streamUntil().get(), offset);
                    exhausted.countDown();
                } else {
                    log.info("No new trips at offset {}", offset);
                }
                return;
            }
            publish(trips);
            offset += trips.size();
            offsets.save(offset);
            log.info("Published {} trips (offset now {})", trips.size(), offset);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("Publishing the page at offset {} failed; it will be retried: {}", offset, e.toString());
        }
    }

    private void publish(List<String> trips) throws Exception {
        List<Future<RecordMetadata>> acks = new ArrayList<>(trips.size());
        for (String trip : trips) {
            acks.add(producer.send(new ProducerRecord<>(settings.topic(), trip)));
        }
        producer.flush();
        for (Future<RecordMetadata> ack : acks) {
            ack.get(); // surfaces any failed send before the offset moves
        }
    }

    void awaitExhausted() throws InterruptedException {
        exhausted.await();
    }

    static Properties kafkaProperties(String brokers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Broker-side dedup of internal retries; keeps per-partition order.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    public static void main(String[] args) throws InterruptedException {
        Map<String, String> env = new HashMap<>(System.getenv());
        if (args.length > 0) {
            env.put("KAFKA_BROKERS", args[0]);
        }
        ProducerSettings settings = ProducerSettings.fromEnv(env);

        SocrataClient source = new SocrataClient(
                settings.socrataEndpoint(), settings.streamFrom(), settings.streamUntil(), settings.appToken());
        KafkaProducer<String, String> kafka = new KafkaProducer<>(kafkaProperties(settings.brokers()));
        TripProducer task = new TripProducer(source, kafka, new OffsetStore(settings.offsetFile()), settings);

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown(scheduler, kafka)));

        log.info("Streaming trips requested from {} to {} into topic {} (resuming at offset {})",
                settings.streamFrom(), settings.streamUntil().orElse("now"), settings.topic(), task.offset);
        scheduler.scheduleWithFixedDelay(task, 0, settings.pollInterval().toMillis(), TimeUnit.MILLISECONDS);
        task.awaitExhausted();
        System.exit(0); // runs the shutdown hook
    }

    private static void shutdown(ScheduledExecutorService scheduler, KafkaProducer<String, String> kafka) {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(2, TimeUnit.MINUTES)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        kafka.close();
    }
}
