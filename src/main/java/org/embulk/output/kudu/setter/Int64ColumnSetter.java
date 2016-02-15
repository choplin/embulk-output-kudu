package org.embulk.output.kudu.setter;

import org.kududb.Type;
import org.kududb.client.PartialRow;

public class Int64ColumnSetter extends ColumnSetter {
    public Int64ColumnSetter(int index) {
        super(index, Type.INT64);
    }

    @Override
    public void setLong(PartialRow row, long v) {
        row.addLong(index, v);
    }
}
