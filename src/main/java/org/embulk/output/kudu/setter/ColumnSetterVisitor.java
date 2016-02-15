package org.embulk.output.kudu.setter;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.kududb.client.PartialRow;

import java.util.List;

public class ColumnSetterVisitor implements ColumnVisitor {
    private final PageReader pageReader;
    private final List<ColumnSetter> setters;
    private final PartialRow row;

    public ColumnSetterVisitor(PageReader pageReader, List<ColumnSetter> setter, PartialRow row) {
        this.pageReader = pageReader;
        this.setters = setter;
        this.row = row;
    }

    @Override
    public void booleanColumn(Column column) {
        ColumnSetter setter = setters.get(column.getIndex());
        if (pageReader.isNull(column)) {
            setter.setNull(row);
        } else {
            setter.setBoolean(row, pageReader.getBoolean(column));
        }
    }

    @Override
    public void longColumn(Column column) {
        ColumnSetter setter = setters.get(column.getIndex());
        if (pageReader.isNull(column)) {
            setter.setNull(row);
        } else {
            setter.setLong(row, pageReader.getLong(column));
        }
    }

    @Override
    public void doubleColumn(Column column) {
        ColumnSetter setter = setters.get(column.getIndex());
        if (pageReader.isNull(column)) {
            setter.setNull(row);
        } else {
            setter.setDouble(row, pageReader.getDouble(column));
        }
    }

    @Override
    public void stringColumn(Column column) {
        ColumnSetter setter = setters.get(column.getIndex());
        if (pageReader.isNull(column)) {
            setter.setNull(row);
        } else {
            setter.setString(row, pageReader.getString(column));
        }
    }

    @Override
    public void timestampColumn(Column column) {
        ColumnSetter setter = setters.get(column.getIndex());
        if (pageReader.isNull(column)) {
            setter.setNull(row);
        } else {
            setter.setTimestamp(row, pageReader.getTimestamp(column));
        }
    }

    @Override
    public void jsonColumn(Column column) {
        ColumnSetter setter = setters.get(column.getIndex());
        if (pageReader.isNull(column)) {
            setter.setNull(row);
        } else {
            setter.setJson(row, pageReader.getJson(column));
        }
    }
}
