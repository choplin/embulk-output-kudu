package org.embulk.output.kudu.setter;

import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.kududb.Type;
import org.kududb.client.PartialRow;
import org.msgpack.value.Value;

public class StringColumnSetter extends ColumnSetter {
    private final TimestampFormatter formatter;

    public StringColumnSetter(int index, TimestampFormatter formatter) {
        super(index, Type.STRING);
        this.formatter = formatter;
    }

    @Override
    public void setBoolean(PartialRow row, boolean v) {
        row.addString(index, String.valueOf(v));
    }

    @Override
    public void setLong(PartialRow row, long v) {
        row.addString(index, String.valueOf(v));
    }

    @Override
    public void setDouble(PartialRow row, double v) {
        row.addString(index, String.valueOf(v));
    }

    @Override
    public void setString(PartialRow row, String v) {
        row.addString(index, String.valueOf(v));
    }

    @Override
    public void setTimestamp(PartialRow row, Timestamp v) {
        row.addString(index, formatter.format(v));
    }

    @Override
    public void setJson(PartialRow row, Value v) {
        row.addString(index, v.toJson());
    }
}
