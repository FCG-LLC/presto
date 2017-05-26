package co.llective.presto.hyena;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static java.util.zip.GZIPInputStream.GZIP_MAGIC;

public class HyenaRecordCursor
        implements RecordCursor
{
    private final List<HyenaColumnHandle> columns;

    private List<String> fields;
    private int foobar = 0;

    public HyenaRecordCursor(HyenaTables localFileTables, List<HyenaColumnHandle> columns, SchemaTableName tableName, HostAddress address, TupleDomain<HyenaColumnHandle> predicate)
    {
        this.columns = requireNonNull(columns, "columns is null");


    }

    private void preparePredicates(TupleDomain<HyenaColumnHandle> predicate) {
        Optional<Map<HyenaColumnHandle, Domain>> domains = predicate.getDomains();
        if (!domains.isPresent()) {
            return;
        }
        // SUMTHIN
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columns.size(), "Invalid field index");
        return columns.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
//        try {
//            fields = reader.readFields();
//        }
//        catch (IOException e) {
//            throw Throwables.propagate(e);
//        }
        return foobar++ < 100;
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return false;
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT, INTEGER);
        return 12L;
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return 0.3;
    }

    @Override
    public Slice getSlice(int field)
    {
        return Slices.utf8Slice("foobar");
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        return false;
    }

    private void checkFieldType(int field, Type... expected)
    {
        Type actual = getType(field);
        for (Type type : expected) {
            if (actual.equals(type)) {
                return;
            }
        }
        String expectedTypes = Joiner.on(", ").join(expected);
        throw new IllegalArgumentException(format("Expected field %s to be type %s but is %s", field, expectedTypes, actual));
    }

    @Override
    public void close()
    {
        // yes
    }

}