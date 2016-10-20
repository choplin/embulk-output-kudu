package org.embulk.output.kudu.setter;

import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;

public class Int32ColumnSetter extends ColumnSetter {
    public Int32ColumnSetter(int index) {
        super(index, Type.INT32);
    }

    @Override
    public void setLong(PartialRow row, long v) {
        if (v > Integer.MAX_VALUE || v < Integer.MIN_VALUE) {
            throw new NotSupportedConversion("long(" + v + ")", type.getName());
        } else {
            row.addInt(index, (int) v);
        }
    }
}
