package org.embulk.output.kudu.setter;

import org.kududb.Type;
import org.kududb.client.PartialRow;

public class Int16ColumnSetter extends ColumnSetter {
    public Int16ColumnSetter(int index) {
        super(index, Type.INT16);
    }

    @Override
    public void setLong(PartialRow row, long v) {
        if (v > Short.MAX_VALUE || v < Short.MIN_VALUE) {
            throw new NotSupportedConversion("long(" + v + ")", type.getName());
        } else {
            row.addShort(index, (short) v);
        }
    }
}
