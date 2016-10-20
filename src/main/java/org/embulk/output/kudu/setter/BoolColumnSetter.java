package org.embulk.output.kudu.setter;

import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;

public class BoolColumnSetter extends ColumnSetter {
    public BoolColumnSetter(int index) {
        super(index, Type.BOOL);
    }

    @Override
    public void setBoolean(PartialRow row, boolean v) {
        row.addBoolean(index, v);
    }
}
