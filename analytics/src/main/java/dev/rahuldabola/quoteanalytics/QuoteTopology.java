package dev.rahuldabola.quoteanalytics;

import dev.rahuldabola.quotes.avro.QuoteRequested;
import dev.rahuldabola.quotes.avro.RegionStats;
import dev.rahuldabola.quotes.avro.RegionTotals;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.WindowStore;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * quote-jobs (JSON) --validate--> quote-requests (Avro, one event per quote)
 *                   \--invalid--> quote-jobs-dlq (original payload + error headers)
 * quote-requests --by region, 1-minute tumbling windows--> quote-region-stats (Avro)
 */
public final class QuoteTopology {

    public static final String JOBS_TOPIC = "quote-jobs";
    public static final String DLQ_TOPIC = "quote-jobs-dlq";
    public static final String REQUESTS_TOPIC = "quote-requests";
    public static final String STATS_TOPIC = "quote-region-stats";
    public static final String TOTALS_STORE = "region-totals";

    public static final String ERROR_HEADER = "dlq.error";
    public static final String SOURCE_TOPIC_HEADER = "dlq.source.topic";

    static final Duration WINDOW = Duration.ofMinutes(1);
    static final Duration GRACE = Duration.ofSeconds(30);

    private QuoteTopology() {
    }

    /** Validation outcome passed from the validator to the branch; never serialized. */
    record Parsed(String raw, List<QuoteRequested> events, String error) {
        boolean ok() {
            return error == null;
        }
    }

    public static Topology build(Map<String, ?> serdeConfig) {
        Serde<String> strings = Serdes.String();
        Serde<QuoteRequested> requestSerde = avroSerde(serdeConfig);
        Serde<RegionTotals> totalsSerde = avroSerde(serdeConfig);
        Serde<RegionStats> statsSerde = avroSerde(serdeConfig);

        StreamsBuilder builder = new StreamsBuilder();
        Map<String, KStream<String, Parsed>> branches = builder
                .stream(JOBS_TOPIC, Consumed.with(strings, strings))
                .processValues(Validate::new, Named.as("validate-job"))
                .split(Named.as("job-"))
                .branch((key, parsed) -> parsed.ok(), Branched.as("valid"))
                .defaultBranch(Branched.as("invalid"));

        // Poison messages keep their original payload so they can be inspected and replayed,
        // and they no longer hold up the rest of the partition.
        branches.get("job-invalid")
                .mapValues(Parsed::raw)
                .to(DLQ_TOPIC, Produced.with(strings, strings));

        KStream<String, QuoteRequested> requests = branches.get("job-valid").flatMapValues(Parsed::events);
        requests.to(REQUESTS_TOPIC, Produced.with(strings, requestSerde));

        requests
                .groupBy((jobId, event) -> event.getRegion(), Grouped.with("by-region", strings, requestSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW, GRACE))
                .aggregate(
                        () -> new RegionTotals(0L, 0L, 0L),
                        (region, event, totals) -> new RegionTotals(
                                totals.getQuoteCount() + 1,
                                totals.getItemCount() + event.getItemCount(),
                                totals.getGrossAmountCents() + event.getGrossAmountCents()),
                        Materialized.<String, RegionTotals, WindowStore<Bytes, byte[]>>as(TOTALS_STORE)
                                .withKeySerde(strings)
                                .withValueSerde(totalsSerde))
                .toStream()
                // Keyed by region + window so the JDBC sink can upsert one row per window.
                .map((window, totals) -> KeyValue.pair(
                        window.key() + "@" + window.window().startTime(),
                        RegionStats.newBuilder()
                                .setRegion(window.key())
                                .setWindowStart(window.window().startTime())
                                .setWindowEnd(window.window().endTime())
                                .setQuoteCount(totals.getQuoteCount())
                                .setItemCount(totals.getItemCount())
                                .setGrossAmountCents(totals.getGrossAmountCents())
                                .build()))
                .to(STATS_TOPIC, Produced.with(strings, statsSerde));

        return builder.build();
    }

    private static <T extends SpecificRecord> Serde<T> avroSerde(Map<String, ?> config) {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(config, false);
        return serde;
    }

    /** Validates each job; invalid ones get headers saying why before they reach the DLQ. */
    static final class Validate implements FixedKeyProcessor<String, String, Parsed> {
        private FixedKeyProcessorContext<String, Parsed> context;

        @Override
        public void init(FixedKeyProcessorContext<String, Parsed> context) {
            this.context = context;
        }

        @Override
        public void process(FixedKeyRecord<String, String> record) {
            try {
                List<QuoteRequested> events = JobParser.parse(record.value());
                context.forward(record.withValue(new Parsed(record.value(), events, null)));
            } catch (InvalidJobException e) {
                FixedKeyRecord<String, Parsed> dead = record.withValue(new Parsed(record.value(), List.of(), e.getMessage()));
                dead.headers().add(ERROR_HEADER, e.getMessage().getBytes(StandardCharsets.UTF_8));
                dead.headers().add(SOURCE_TOPIC_HEADER, JOBS_TOPIC.getBytes(StandardCharsets.UTF_8));
                context.forward(dead);
            }
        }
    }
}
