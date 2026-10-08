package dev.rahuldabola.quoteanalytics;

import dev.rahuldabola.quotes.avro.QuoteRequested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobParserTest {

    // Same shape the quote-api function publishes: Decimal fields arrive as strings.
    static final String VALID_JOB = """
            {"job_id": "job-1", "quotes": [
              {"items": [{"sku": "A-1", "qty": 3, "unit_price": "19.99"},
                         {"sku": "B-2", "qty": 1, "unit_price": "5.00"}],
               "region": "IN", "coupon": "SAVE10"},
              {"items": [{"sku": "C-3", "qty": 2, "unit_price": 100}], "region": "US", "coupon": null}
            ]}""";

    @Test
    void parsesOneEventPerQuoteWithGrossInCents() throws InvalidJobException {
        List<QuoteRequested> events = JobParser.parse(VALID_JOB);

        assertEquals(2, events.size());
        QuoteRequested first = events.get(0);
        assertEquals("job-1", first.getJobId());
        assertEquals(0, first.getPosition());
        assertEquals("IN", first.getRegion());
        assertEquals(2, first.getItemCount());
        assertEquals(3 * 1999 + 500, first.getGrossAmountCents());
        assertEquals("SAVE10", first.getCoupon());

        QuoteRequested second = events.get(1);
        assertEquals(1, second.getPosition());
        assertEquals(20_000, second.getGrossAmountCents());
        assertNull(second.getCoupon());
    }

    @Test
    void largestAllowedJobDoesNotOverflow() throws InvalidJobException {
        String item = "{\"sku\": \"X\", \"qty\": 1000, \"unit_price\": \"100000.00\"}";
        String items = String.join(",", java.util.Collections.nCopies(100, item));
        String job = "{\"job_id\": \"big\", \"quotes\": [{\"items\": [" + items + "], \"region\": \"EU\"}]}";

        assertEquals(100L * 1000 * 10_000_000, JobParser.parse(job).get(0).getGrossAmountCents());
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
            "malformed JSON        | {\"job_id\": ",
            "not an object         | [1, 2]",
            "missing job_id        | {\"quotes\": [{\"items\": [{\"sku\": \"A\", \"qty\": 1, \"unit_price\": \"1\"}], \"region\": \"IN\"}]}",
            "empty quotes          | {\"job_id\": \"j\", \"quotes\": []}",
            "unknown region        | {\"job_id\": \"j\", \"quotes\": [{\"items\": [{\"sku\": \"A\", \"qty\": 1, \"unit_price\": \"1\"}], \"region\": \"UK\"}]}",
            "no items              | {\"job_id\": \"j\", \"quotes\": [{\"items\": [], \"region\": \"IN\"}]}",
            "zero qty              | {\"job_id\": \"j\", \"quotes\": [{\"items\": [{\"sku\": \"A\", \"qty\": 0, \"unit_price\": \"1\"}], \"region\": \"IN\"}]}",
            "fractional qty        | {\"job_id\": \"j\", \"quotes\": [{\"items\": [{\"sku\": \"A\", \"qty\": 1.5, \"unit_price\": \"1\"}], \"region\": \"IN\"}]}",
            "three-decimal price   | {\"job_id\": \"j\", \"quotes\": [{\"items\": [{\"sku\": \"A\", \"qty\": 1, \"unit_price\": \"1.999\"}], \"region\": \"IN\"}]}",
            "negative price        | {\"job_id\": \"j\", \"quotes\": [{\"items\": [{\"sku\": \"A\", \"qty\": 1, \"unit_price\": \"-1\"}], \"region\": \"IN\"}]}",
            "price not a number    | {\"job_id\": \"j\", \"quotes\": [{\"items\": [{\"sku\": \"A\", \"qty\": 1, \"unit_price\": \"abc\"}], \"region\": \"IN\"}]}",
            "empty sku             | {\"job_id\": \"j\", \"quotes\": [{\"items\": [{\"sku\": \"\", \"qty\": 1, \"unit_price\": \"1\"}], \"region\": \"IN\"}]}",
    })
    void rejectsPoisonMessages(String reason, String payload) {
        InvalidJobException error = assertThrows(InvalidJobException.class, () -> JobParser.parse(payload), reason);
        assertTrue(!error.getMessage().isBlank());
    }
}
