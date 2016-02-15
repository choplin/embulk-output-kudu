package org.embulk.output.kudu.setter;

import org.kududb.Type;
import org.kududb.client.PartialRow;

public class DoubleColumnSetter extends ColumnSetter {
    public DoubleColumnSetter(int index) {
        super(index, Type.DOUBLE);
    }

    @Override
    public void setDouble(PartialRow row, double v) {
        row.addDouble(index, v);
    }
}
