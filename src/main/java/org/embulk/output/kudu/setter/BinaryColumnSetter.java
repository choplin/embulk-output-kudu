package org.embulk.output.kudu.setter;

import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;

import java.nio.charset.Charset;

public class BinaryColumnSetter extends ColumnSetter {
    private final Charset charset = Charset.defaultCharset();

    public BinaryColumnSetter(int index) {
        super(index, Type.BINARY);
    }

    @Override
    public void setString(PartialRow row, String v) {
        row.addBinary(index, v.getBytes(charset));
    }
}
