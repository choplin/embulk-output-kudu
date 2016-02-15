package org.embulk.output.kudu.setter;

import org.kududb.Type;
import org.kududb.client.PartialRow;

public class BoolColumnSetter extends ColumnSetter {
    public BoolColumnSetter(int index) {
        super(index, Type.BOOL);
    }

    @Override
    public void setBoolean(PartialRow row, boolean v) {
        row.addBoolean(index, v);
    }
}
