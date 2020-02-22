package com.biubiu.engine;

import com.biubiu.config.TargetConfig;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTimeZone;

import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.biubiu.engine.Utils.quoted;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.readBigDecimal;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Collections.nCopies;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.joda.time.chrono.ISOChronology.getInstanceUTC;

public class JdbcPageSink implements ConnectorPageSink {
    private Connection connection;
    private PreparedStatement statement;
    private List<Type> columnTypes;
    private TargetConfig targetConfig;
    private int batchSize;
    private List<String> sourceColumns;

    public JdbcPageSink(Boolean off, TargetConfig targetConfig, List<JdbcColumnHandle> columnHandles, Connection connection) {
        this.connection = connection;
        this.targetConfig = targetConfig;
        this.sourceColumns = columnHandles.stream().map(JdbcColumnHandle::getColumnName).collect(Collectors.toList());
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (JdbcColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        try {
            if (off) {
                Connection conn = Utils.createConnection(targetConfig.getUrl(), targetConfig.getUsername(), targetConfig.getPassword());
                String sql = "select COLUMN_NAME from information_schema.COLUMNS where table_name = ? and table_schema = ?;";
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.setString(1, targetConfig.getTable());
                stmt.setString(2, targetConfig.getDatabase());
                ResultSet rs = stmt.executeQuery();
                List<String> list = Collections.synchronizedList(new ArrayList<>());
                while (rs.next()) {
                    list.add(rs.getString("COLUMN_NAME"));
                }
                Utils.close(conn, stmt, rs);
                // 记录缺少的字段
                Set<String> lack = Collections.synchronizedSet(new HashSet<>());
                for (JdbcColumnHandle temp : columnHandles) {
                    if (!list.contains(temp.getColumnName())) {
                        lack.add(temp.getColumnName());
                    }
                }
                if (lack.size() != 0) {
                    throw new RuntimeException("sql error , Exception: 目标表缺少必要字段【" + StringUtils.join(lack, ",") + "】....");
                }
            }
            this.connection.setAutoCommit(false);
            statement = this.connection.prepareStatement(off ? buildInsertSql() : buildInsertSql(columnHandles));
            System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " Executing: " + statement.toString());
        } catch (SQLException e) {
            closeWithSuppression(connection, e);
            throw new RuntimeException("sql error " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("ObjectEquality")
    private static void closeWithSuppression(Connection connection, Throwable throwable) {
        try {
            connection.close();
        } catch (Throwable t) {
            // Self-suppression not permitted
            if (throwable != t) {
                throwable.addSuppressed(t);
            }
        }
    }

    public String buildInsertSql() {
        String vars = com.google.common.base.Joiner.on(',').join(nCopies(columnTypes.size(), "?"));
        return new StringBuilder()
                .append("INSERT INTO ")
                .append(quoted(targetConfig.getDatabase(), targetConfig.getTable(), Utils.MYSQL_IDENTIFIER_QUOTE))
                .append(" VALUES (").append(vars).append(")")
                .toString();
    }

    public String buildInsertSql(List<JdbcColumnHandle> columnHandles) {
        List<String> columnNames = columnHandles.stream().map(JdbcColumnHandle::getColumnName).collect(Collectors.toList());
        String columns = StringUtils.join(columnNames, " ,");
        String vars = com.google.common.base.Joiner.on(',').join(nCopies(columnTypes.size(), "?"));
        System.out.println("vars: " + vars);
        return new StringBuilder()
                .append("INSERT INTO ")
                .append(quoted(targetConfig.getDatabase(), targetConfig.getTable(), Utils.MYSQL_IDENTIFIER_QUOTE))
                .append(" (").append(columns).append(")")
                .append(" VALUES (").append(vars).append(")")
                .toString();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page) {
        try {
            for (int position = 0; position < page.getPositionCount(); position++) {
                int channel;
                for (channel = 0; channel < page.getChannelCount(); channel++) {
                    appendColumn(page, position, channel);
                }
                statement.addBatch();
                batchSize++;
                if (batchSize >= 1000) {
                    statement.executeBatch();
                    connection.commit();
                    connection.setAutoCommit(false);
                    batchSize = 0;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("sql error ", e);
        }
        return NOT_BLOCKED;
    }

    private void appendColumn(Page page, int position, int channel) throws SQLException {
        Block block = page.getBlock(channel);
        int parameter = channel + 1;

        if (block.isNull(position)) {
            statement.setObject(parameter, null);
            return;
        }

        Type type = columnTypes.get(channel);
        if (BOOLEAN.equals(type)) {
            statement.setBoolean(parameter, type.getBoolean(block, position));
        } else if (BIGINT.equals(type)) {
            statement.setLong(parameter, type.getLong(block, position));
        } else if (INTEGER.equals(type)) {
            statement.setInt(parameter, toIntExact(type.getLong(block, position)));
        } else if (SMALLINT.equals(type)) {
            statement.setShort(parameter, Shorts.checkedCast(type.getLong(block, position)));
        } else if (TINYINT.equals(type)) {
            statement.setByte(parameter, SignedBytes.checkedCast(type.getLong(block, position)));
        } else if (DOUBLE.equals(type)) {
            statement.setDouble(parameter, type.getDouble(block, position));
        } else if (REAL.equals(type)) {
            statement.setFloat(parameter, intBitsToFloat(toIntExact(type.getLong(block, position))));
        } else if (type instanceof DecimalType) {
            statement.setBigDecimal(parameter, readBigDecimal((DecimalType) type, block, position));
        } else if (isVarcharType(type) || isCharType(type)) {
            statement.setString(parameter, type.getSlice(block, position).toStringUtf8());
        } else if (VARBINARY.equals(type)) {
            statement.setBytes(parameter, type.getSlice(block, position).getBytes());
        } else if (DATE.equals(type)) {
            // convert to midnight in default time zone
            long utcMillis = DAYS.toMillis(type.getLong(block, position));
            long localMillis = getInstanceUTC().getZone().getMillisKeepLocal(DateTimeZone.getDefault(), utcMillis);
            statement.setDate(parameter, new Date(localMillis));
        } else if (type instanceof TimestampType) {
            statement.setTimestamp(parameter, new Timestamp(type.getLong(block, position)));
        } else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        // commit and close
        try (Connection connection = this.connection;
             PreparedStatement statement = this.statement) {
            if (batchSize > 0) {
                statement.executeBatch();
                connection.commit();
            }
        } catch (SQLNonTransientException e) {
            throw new RuntimeException("non trans error ", e);
        } catch (SQLException e) {
            throw new RuntimeException("sql error ", e);
        }
        // the committer does not need any additional info
        return completedFuture(ImmutableList.of());
    }

    @SuppressWarnings("unused")
    @Override
    public void abort() {
        // rollback and close
        try (Connection connection = this.connection;
             PreparedStatement statement = this.statement) {
            connection.rollback();
        } catch (SQLException e) {
            throw new RuntimeException("sql error ", e);
        }
    }
}
