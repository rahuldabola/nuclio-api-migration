package dev.rahuldabola.quoteanalytics;

/** A quote-jobs message that can never be processed; it is routed to the dead-letter topic. */
public class InvalidJobException extends Exception {
    public InvalidJobException(String message) {
        super(message);
    }
}
