package org.embulk.output.kudu.setter;

import org.embulk.spi.time.Timestamp;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.msgpack.value.Value;

public abstract class ColumnSetter {
    protected final int index;
    protected final Type type;

    public ColumnSetter(int index, Type type) {
        this.index = index;
        this.type = type;
    }

    public void setNull(PartialRow row) {
        row.setNull(index);
    }

    public void setBoolean(PartialRow row, boolean v) {
        throw new NotSupportedConversion("boolean", type.getName());
    }

    public void setLong(PartialRow row, long v) {
        throw new NotSupportedConversion("long", type.getName());
    }

    public void setDouble(PartialRow row, double v) {
        throw new NotSupportedConversion("double", type.getName());
    }

    public void setString(PartialRow row, String v) {
        throw new NotSupportedConversion("string", type.getName());
    }

    public void setTimestamp(PartialRow row, Timestamp v) {
        throw new NotSupportedConversion("timestamp", type.getName());
    }

    public void setJson(PartialRow row, Value v) {
        throw new NotSupportedConversion("json", type.getName());
    }

}
