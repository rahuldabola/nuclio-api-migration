package dev.rahuldabola.quoteanalytics;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/** Runs the quote analytics topology with exactly-once processing. Configured through env vars. */
public final class AnalyticsApp {

    private static final Logger LOG = LoggerFactory.getLogger(AnalyticsApp.class);

    private AnalyticsApp() {
    }

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, env("APPLICATION_ID", "quote-analytics"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env("BOOTSTRAP_SERVERS", "kafka:9092"));
        // Transactions commit input offsets, state-store changelog writes and output records
        // together, so a crash and restart never double-counts a quote in the window totals.
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, Integer.parseInt(env("REPLICATION_FACTOR", "1")));
        props.put(StreamsConfig.STATE_DIR_CONFIG, env("STATE_DIR", "/tmp/kafka-streams"));
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

        Topology topology = QuoteTopology.build(Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")));
        LOG.info("Topology:\n{}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch stopped = new CountDownLatch(1);
        streams.setStateListener((now, before) -> {
            LOG.info("State {} -> {}", before, now);
            if (now == KafkaStreams.State.ERROR || now == KafkaStreams.State.NOT_RUNNING) {
                stopped.countDown();
            }
        });
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close, "shutdown"));

        streams.start();
        stopped.await();
        if (streams.state() == KafkaStreams.State.ERROR) {
            System.exit(1);
        }
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
