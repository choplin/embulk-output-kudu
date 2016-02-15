package org.embulk.output.kudu.setter;

import org.kududb.Type;
import org.kududb.client.PartialRow;

public class Int8ColumnSetter extends ColumnSetter {
    public Int8ColumnSetter(int index) {
        super(index, Type.INT8);
    }

    @Override
    public void setLong(PartialRow row, long v) {
        if (v > Byte.MAX_VALUE || v < Byte.MIN_VALUE) {
            throw new NotSupportedConversion("long(" + v + ")", type.getName());
        } else {
            row.addByte(index, (byte) v);
        }
    }
}
