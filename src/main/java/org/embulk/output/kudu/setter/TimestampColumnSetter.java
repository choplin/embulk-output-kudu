package org.embulk.output.kudu.setter;

import org.embulk.spi.time.Timestamp;
import org.kududb.Type;
import org.kududb.client.PartialRow;

public class TimestampColumnSetter extends ColumnSetter {
    public TimestampColumnSetter(int index) {
        super(index, Type.TIMESTAMP);
    }

    @Override
    public void setTimestamp(PartialRow row, Timestamp v) {
        row.addLong(index, toMicroSeconds(v.getEpochSecond(), v.getNano()));
    }

    private long toMicroSeconds(long epoch, int nano) {
        return epoch * 1000L * 1000L + nano / 1000L;
    }
}
