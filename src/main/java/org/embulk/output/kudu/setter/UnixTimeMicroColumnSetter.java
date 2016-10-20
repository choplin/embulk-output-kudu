package org.embulk.output.kudu.setter;

import org.embulk.spi.time.Timestamp;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;

public class UnixTimeMicroColumnSetter extends ColumnSetter {
    public UnixTimeMicroColumnSetter(int index) {
        super(index, Type.UNIXTIME_MICROS);
    }

    @Override
    public void setTimestamp(PartialRow row, Timestamp v) {
        row.addLong(index, toMicroSeconds(v.getEpochSecond(), v.getNano()));
    }

    private long toMicroSeconds(long epoch, int nano) {
        return epoch * 1000L * 1000L + nano / 1000L;
    }
}
