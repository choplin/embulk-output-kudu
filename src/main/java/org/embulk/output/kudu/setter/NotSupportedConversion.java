package org.embulk.output.kudu.setter;

public class NotSupportedConversion extends RuntimeException {
    public NotSupportedConversion(String from, String to) {
        super("Conversion from " + from + " to " + to + " is not supported");
    }
}
