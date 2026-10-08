package dev.rahuldabola.quoteanalytics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.rahuldabola.quotes.avro.QuoteRequested;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Parses a quote-jobs message ({"job_id": ..., "quotes": [...]}) into one event per quote.
 *
 * The rules mirror quotecore.models.QuoteRequest, the Python contract the API validates
 * with, so anything the API accepted parses here and anything else is a poison message.
 */
public final class JobParser {

    static final Set<String> REGIONS = Set.of("IN", "US", "EU");
    private static final BigDecimal MAX_UNIT_PRICE = new BigDecimal("100000");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JobParser() {
    }

    public static List<QuoteRequested> parse(String json) throws InvalidJobException {
        if (json == null) {
            throw new InvalidJobException("empty message");
        }
        JsonNode root;
        try {
            root = MAPPER.readTree(json);
        } catch (JsonProcessingException e) {
            throw new InvalidJobException("malformed JSON");
        }
        if (root == null || !root.isObject()) {
            throw new InvalidJobException("message is not a JSON object");
        }

        String jobId = text(root, "job_id");
        if (jobId == null || jobId.isBlank()) {
            throw new InvalidJobException("missing job_id");
        }
        JsonNode quotes = root.get("quotes");
        if (quotes == null || !quotes.isArray() || quotes.isEmpty() || quotes.size() > 50) {
            throw new InvalidJobException("quotes must be a list of 1-50 requests");
        }

        List<QuoteRequested> events = new ArrayList<>(quotes.size());
        for (int i = 0; i < quotes.size(); i++) {
            events.add(parseQuote(jobId, i, quotes.get(i)));
        }
        return events;
    }

    private static QuoteRequested parseQuote(String jobId, int position, JsonNode quote) throws InvalidJobException {
        String where = "quotes[" + position + "]";
        if (!quote.isObject()) {
            throw new InvalidJobException(where + " is not an object");
        }
        String region = text(quote, "region");
        if (region == null || !REGIONS.contains(region)) {
            throw new InvalidJobException(where + ".region must be IN, US or EU");
        }
        JsonNode items = quote.get("items");
        if (items == null || !items.isArray() || items.isEmpty() || items.size() > 100) {
            throw new InvalidJobException(where + ".items must be a list of 1-100 items");
        }

        long grossCents = 0;
        for (int j = 0; j < items.size(); j++) {
            grossCents += lineTotalCents(items.get(j), where + ".items[" + j + "]");
        }

        JsonNode coupon = quote.get("coupon");
        if (coupon != null && !coupon.isNull() && (!coupon.isTextual() || coupon.asText().length() > 32)) {
            throw new InvalidJobException(where + ".coupon must be a string of at most 32 characters");
        }

        return QuoteRequested.newBuilder()
                .setJobId(jobId)
                .setPosition(position)
                .setRegion(region)
                .setItemCount(items.size())
                .setGrossAmountCents(grossCents)
                .setCoupon(coupon == null || coupon.isNull() ? null : coupon.asText())
                .build();
    }

    private static long lineTotalCents(JsonNode item, String where) throws InvalidJobException {
        if (!item.isObject()) {
            throw new InvalidJobException(where + " is not an object");
        }
        String sku = text(item, "sku");
        if (sku == null || sku.isEmpty() || sku.length() > 64) {
            throw new InvalidJobException(where + ".sku must be 1-64 characters");
        }
        JsonNode qty = item.get("qty");
        if (qty == null || !qty.isIntegralNumber() || !qty.canConvertToInt() || qty.asInt() < 1 || qty.asInt() > 1000) {
            throw new InvalidJobException(where + ".qty must be an integer from 1 to 1000");
        }
        BigDecimal unitPrice = decimal(item.get("unit_price"));
        if (unitPrice == null || unitPrice.signum() <= 0 || unitPrice.compareTo(MAX_UNIT_PRICE) > 0
                || unitPrice.stripTrailingZeros().scale() > 2) {
            throw new InvalidJobException(where + ".unit_price must be > 0 and <= 100000 with at most 2 decimals");
        }
        // Bounded by the checks above: at most 100 items * 1000 qty * 10,000,000 cents.
        return unitPrice.movePointRight(2).longValueExact() * qty.asInt();
    }

    // The API serializes Decimal fields as strings ("19.99"); accept plain JSON numbers too.
    private static BigDecimal decimal(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        try {
            if (node.isTextual()) {
                return new BigDecimal(node.asText());
            }
            return node.isNumber() ? node.decimalValue() : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value != null && value.isTextual() ? value.asText() : null;
    }
}
