package dev.rahuldabola.quoteanalytics;

import dev.rahuldabola.quotes.avro.QuoteRequested;
import dev.rahuldabola.quotes.avro.RegionStats;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuoteTopologyTest {

    private static final String SCOPE = "quote-topology-test";
    private static final Map<String, String> SERDE_CONFIG =
            Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://" + SCOPE);
    private static final Instant T0 = Instant.parse("2026-10-09T10:00:05Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> jobs;
    private TestOutputTopic<String, String> dlq;
    private TestOutputTopic<String, QuoteRequested> requests;
    private TestOutputTopic<String, RegionStats> stats;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "quote-analytics-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        driver = new TopologyTestDriver(QuoteTopology.build(SERDE_CONFIG), props);

        jobs = driver.createInputTopic(QuoteTopology.JOBS_TOPIC,
                Serdes.String().serializer(), Serdes.String().serializer());
        dlq = driver.createOutputTopic(QuoteTopology.DLQ_TOPIC,
                Serdes.String().deserializer(), Serdes.String().deserializer());
        requests = driver.createOutputTopic(QuoteTopology.REQUESTS_TOPIC,
                Serdes.String().deserializer(), QuoteTopologyTest.<QuoteRequested>avro().deserializer());
        stats = driver.createOutputTopic(QuoteTopology.STATS_TOPIC,
                Serdes.String().deserializer(), QuoteTopologyTest.<RegionStats>avro().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
        MockSchemaRegistry.dropScope(SCOPE);
    }

    @Test
    void validJobFansOutIntoAvroEventsKeyedByJob() {
        jobs.pipeInput("job-1", JobParserTest.VALID_JOB, T0);

        List<TestRecord<String, QuoteRequested>> out = requests.readRecordsToList();
        assertEquals(2, out.size());
        assertEquals("job-1", out.get(0).key());
        assertEquals("IN", out.get(0).value().getRegion());
        assertEquals("US", out.get(1).value().getRegion());
        assertTrue(dlq.isEmpty());
    }

    @Test
    void poisonMessageGoesToDlqWithReasonAndDoesNotBlockLaterJobs() {
        String poison = "{\"job_id\": \"bad\", \"quotes\": [{\"items\": [], \"region\": \"IN\"}]}";
        jobs.pipeInput("bad", poison, T0);
        jobs.pipeInput("job-1", JobParserTest.VALID_JOB, T0.plusSeconds(1));

        TestRecord<String, String> dead = dlq.readRecord();
        assertEquals("bad", dead.key());
        assertEquals(poison, dead.value());
        assertEquals("quotes[0].items must be a list of 1-100 items", header(dead, QuoteTopology.ERROR_HEADER));
        assertEquals(QuoteTopology.JOBS_TOPIC, header(dead, QuoteTopology.SOURCE_TOPIC_HEADER));
        assertTrue(dlq.isEmpty());

        assertEquals(2, requests.readRecordsToList().size());
    }

    @Test
    void aggregatesPerRegionPerOneMinuteWindow() {
        jobs.pipeInput("job-1", JobParserTest.VALID_JOB, T0);
        jobs.pipeInput("job-2", JobParserTest.VALID_JOB, T0.plusSeconds(20));
        jobs.pipeInput("job-3", JobParserTest.VALID_JOB, T0.plusSeconds(70));

        Map<String, RegionStats> latest = stats.readKeyValuesToMap();
        Instant firstWindow = Instant.parse("2026-10-09T10:00:00Z");
        Instant secondWindow = Instant.parse("2026-10-09T10:01:00Z");

        RegionStats in = latest.get("IN@" + firstWindow);
        assertEquals(2, in.getQuoteCount());
        assertEquals(4, in.getItemCount());
        assertEquals(2 * (3 * 1999 + 500), in.getGrossAmountCents());
        assertEquals(firstWindow, in.getWindowStart());
        assertEquals(secondWindow, in.getWindowEnd());

        assertEquals(2, latest.get("US@" + firstWindow).getQuoteCount());
        assertEquals(1, latest.get("IN@" + secondWindow).getQuoteCount());
        assertEquals(4, latest.size());
    }

    @Test
    void lateEventsInsideGraceAreCountedAndAfterGraceAreDropped() {
        Instant firstWindow = Instant.parse("2026-10-09T10:00:00Z");
        jobs.pipeInput("job-1", JobParserTest.VALID_JOB, T0);
        // Stream time 10:01:20 is still inside the 30 s grace of the 10:00 window.
        jobs.pipeInput("job-2", JobParserTest.VALID_JOB, firstWindow.plusSeconds(80));
        jobs.pipeInput("late-ok", JobParserTest.VALID_JOB, T0.plusSeconds(10));
        // Stream time 10:02:00 closes the 10:00 window for good.
        jobs.pipeInput("job-3", JobParserTest.VALID_JOB, firstWindow.plusSeconds(120));
        jobs.pipeInput("too-late", JobParserTest.VALID_JOB, T0.plusSeconds(20));

        Map<String, RegionStats> latest = stats.readKeyValuesToMap();
        assertEquals(2, latest.get("IN@" + firstWindow).getQuoteCount());
    }

    private static String header(TestRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static <T extends org.apache.avro.specific.SpecificRecord> SpecificAvroSerde<T> avro() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(SERDE_CONFIG, false);
        return serde;
    }
}
