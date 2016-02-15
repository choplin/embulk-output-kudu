package org.embulk.output.kudu.setter;

import org.embulk.spi.time.Timestamp;
import org.kududb.client.PartialRow;
import org.msgpack.value.Value;

public class SkipColumnSetter extends ColumnSetter {
    public SkipColumnSetter() {
        super(0, null);
    }

    @Override
    public void setNull(PartialRow row) {
    }

    @Override
    public void setBoolean(PartialRow row, boolean v) {
    }

    @Override
    public void setLong(PartialRow row, long v) {
    }

    @Override
    public void setDouble(PartialRow row, double v) {
    }

    @Override
    public void setString(PartialRow row, String v) {
    }

    @Override
    public void setTimestamp(PartialRow row, Timestamp v) {
    }

    @Override
    public void setJson(PartialRow row, Value v) {
    }
}
