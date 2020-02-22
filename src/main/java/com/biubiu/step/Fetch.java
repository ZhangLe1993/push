package com.biubiu.step;

import com.biubiu.config.SourceConfig;
import com.biubiu.engine.JdbcColumnHandle;
import com.biubiu.engine.JdbcTypeHandle;
import com.biubiu.engine.ReadMapping;
import com.biubiu.engine.StandardReadMappings;
import com.biubiu.model.SourceSchema;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.biubiu.engine.Utils.close;
import static com.biubiu.engine.Utils.createConnection;
import static com.biubiu.engine.Utils.escapeNamePattern;

/**
 * @author yule.zhang
 * @date 2020/2/22 19:46
 * @email zhangyule1993@sina.com
 * @description 第一步获取信息，类名取自与gitlab fetch命令类似的含义
 */
public class Fetch {
    /**
     * 获取源库要同步的表的字段信息
     *
     * @throws SQLException
     */
    public SourceSchema getColumns(SourceConfig sourceConfig) throws Exception {
        String fullSchemaName = sourceConfig.getCatalog() + "." + sourceConfig.getSchema();
        SchemaTableName fullName = new SchemaTableName(fullSchemaName, sourceConfig.getTable());
        SourceSchema sourceSchema = new SourceSchema();
        sourceSchema.setFullName(fullName);
        Connection connection = null;
        ResultSet resultSet = null;
        try {
            connection = createConnection(sourceConfig.getUrl(), sourceConfig.getUsername(), null);
            resultSet = getColumns(connection.getMetaData(), sourceConfig.getCatalog(), sourceConfig.getSchema(), sourceConfig.getTable());
            List<JdbcColumnHandle> columns = new ArrayList<>();
            while (resultSet.next()) {
                JdbcTypeHandle typeHandle = new JdbcTypeHandle(resultSet.getInt("DATA_TYPE"), resultSet.getInt("COLUMN_SIZE"), resultSet.getInt("DECIMAL_DIGITS"));
                Optional<ReadMapping> columnMapping = toPrestoType(typeHandle);
                // skip unsupported column types
                if (columnMapping.isPresent()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    if (!"aba_seq".equalsIgnoreCase(columnName)) {
                        columns.add(new JdbcColumnHandle(sourceConfig.getSchema(), columnName, typeHandle, columnMapping.get().getType()));
                    }
                }
            }
            if (columns.isEmpty()) {
                throw new TableNotFoundException(fullName);
            }
            sourceSchema.setColumnHandleList(columns);
            ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builder();
            for (JdbcColumnHandle column : columns) {
                columnMetadata.add(column.getColumnMetadata());
            }
            sourceSchema.setColumnMetadataList(columnMetadata.build());
        } catch (Throwable e) {
            throw e;
        } finally {
            close(connection, null, resultSet);
        }
        return sourceSchema;
    }

    private ResultSet getColumns(DatabaseMetaData metadata, String catalog, String schema, String table) throws SQLException {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(catalog, escapeNamePattern(schema, escape), escapeNamePattern(table, escape), null);
    }


    public Optional<ReadMapping> toPrestoType(JdbcTypeHandle typeHandle) {
        return StandardReadMappings.jdbcTypeToPrestoType(typeHandle);
    }
}
