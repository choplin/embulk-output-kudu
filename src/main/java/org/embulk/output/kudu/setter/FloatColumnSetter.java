package org.embulk.output.kudu.setter;

import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;

public class FloatColumnSetter extends ColumnSetter {
    public FloatColumnSetter(int index) {
        super(index, Type.FLOAT);
    }

    @Override
    public void setDouble(PartialRow row, double v) {
        row.addFloat(index, (float) v);
    }
}
