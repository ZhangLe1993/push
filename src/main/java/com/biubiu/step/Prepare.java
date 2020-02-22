package com.biubiu.step;

import com.biubiu.config.SourceConfig;
import com.biubiu.config.TargetConfig;
import com.biubiu.engine.Utils;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

/**
 * @author yule.zhang
 * @date 2020/2/22 19:59
 * @email zhangyule1993@sina.com
 * @description 插入数据前的准备，例如删除目标表，清空目标表
 */
public class Prepare {

    public static final String MYSQL_IDENTIFIER_QUOTE = "`";

    public static final Map<Type, String> SQL_TYPES = ImmutableMap.<Type, String>builder()
            .put(BOOLEAN, "boolean")
            .put(BIGINT, "bigint")
            .put(INTEGER, "integer")
            .put(SMALLINT, "smallint")
            .put(TINYINT, "tinyint")
            .put(DOUBLE, "double precision")
            .put(REAL, "real")
            .put(VARBINARY, "varbinary")
            .put(DATE, "date")
            .put(TIME, "time")
            .put(TIME_WITH_TIME_ZONE, "time with timezone")
            .put(TIMESTAMP, "timestamp")
            .put(TIMESTAMP_WITH_TIME_ZONE, "timestamp with timezone")
            .build();

    public void truncateTable(TargetConfig targetConfig) throws Exception {
        String schema = targetConfig.getDatabase();
        String tableName = targetConfig.getTable();
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = Utils.createConnection(targetConfig.getUrl(), targetConfig.getUsername(), targetConfig.getPassword());
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            if (uppercase) {
                schema = schema.toUpperCase(Locale.ENGLISH);
                tableName = tableName.toUpperCase(Locale.ENGLISH);
            }
            StringBuilder sql = new StringBuilder()
                    .append("DROP TABLE IF EXISTS ")
                    .append(Utils.quoted(schema, tableName, MYSQL_IDENTIFIER_QUOTE))
                    .append(";");
            System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " sql: " + sql.toString());
            statement = connection.createStatement();
            statement.execute(sql.toString());
        } catch (Throwable e) {
            throw e;
        } finally {
            Utils.close(connection, statement, resultSet);
        }
    }


    public void createTableTableInMysql(List<ColumnMetadata> columnMetadataList, TargetConfig targetConfig) throws Exception {
        String schema = targetConfig.getDatabase();
        String tableName = targetConfig.getTable();
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = Utils.createConnection(targetConfig.getUrl(), targetConfig.getUsername(), targetConfig.getPassword());
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            if (uppercase) {
                schema = schema.toUpperCase(ENGLISH);
                tableName = tableName.toUpperCase(ENGLISH);
            }
            StringBuilder sql = new StringBuilder()
                    .append("CREATE TABLE IF NOT EXISTS ")
                    .append(Utils.quoted(schema, tableName, MYSQL_IDENTIFIER_QUOTE))
                    .append(" (");
            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            for (ColumnMetadata column : columnMetadataList) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                columnList.add(new StringBuilder()
                        .append(Utils.quoted(columnName, MYSQL_IDENTIFIER_QUOTE))
                        .append(" ")
                        .append(toMysqlSqlType(column.getType()))
                        .toString());
            }
            Joiner.on(", ").appendTo(sql, columnList.build());
            sql.append(")");
            System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " sql: " + sql.toString());
            statement = connection.createStatement();
            statement.execute(sql.toString());
        } catch (Throwable e) {
            throw e;
        } finally {
            Utils.close(connection, statement, resultSet);
        }
    }


    private String toMysqlSqlType(Type type) {
        if (REAL.equals(type)) {
            return "float";
        }
        if (TIME_WITH_TIME_ZONE.equals(type) || TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
        if (TIMESTAMP.equals(type)) {
            return "datetime";
        }
        if (VARBINARY.equals(type)) {
            return "mediumblob";
        }
        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded()) {
                return "longtext";
            }
            if (varcharType.getLengthSafe() <= 255) {
                return "tinytext";
            }
            if (varcharType.getLengthSafe() <= 65535) {
                return "text";
            }
            if (varcharType.getLengthSafe() <= 16777215) {
                return "mediumtext";
            }
            return "longtext";
        }

        return toSqlType(type);
    }

    private String toSqlType(Type type) {
        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded()) {
                return "varchar";
            }
            return "varchar(" + varcharType.getLengthSafe() + ")";
        }
        if (type instanceof CharType) {
            if (((CharType) type).getLength() == CharType.MAX_LENGTH) {
                return "char";
            }
            return "char(" + ((CharType) type).getLength() + ")";
        }
        if (type instanceof DecimalType) {
            return format("decimal(%s, %s)", ((DecimalType) type).getPrecision(), ((DecimalType) type).getScale());
        }

        String sqlType = SQL_TYPES.get(type);
        if (sqlType != null) {
            return sqlType;
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }
}
