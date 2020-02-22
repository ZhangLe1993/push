package com.biubiu.engine;

import com.biubiu.config.SourceConfig;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.util.List;

public class JdbcRecordSet implements RecordSet {
    private List<Type> columnTypes;
    private List<JdbcColumnHandle> columnHandles;
    private SourceConfig sourceMeta;
    private Connection connection;

    public JdbcRecordSet(SourceConfig sourceMeta, List<JdbcColumnHandle> columnHandles, Connection connection) {
        this.sourceMeta = sourceMeta;
        this.columnHandles = columnHandles;
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (JdbcColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        this.connection = connection;
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new JdbcRecordCursor(sourceMeta, columnHandles, connection);
    }
}
