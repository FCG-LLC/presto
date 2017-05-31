package co.llective.presto.hyena;

import co.llective.presto.hyena.api.HyenaApi;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.commons.lang3.StringUtils;
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
    private final Long partitionId;
    private final TupleDomain<HyenaColumnHandle> predicate;
    private final HyenaSession hyenaSession;


    private List<String> fields;
    private int foobar = 0;

    private final HyenaApi.ScanResult result;
    private int rowPosition;

    public HyenaRecordCursor(HyenaSession hyenaSession, List<HyenaColumnHandle> columns, HostAddress address, Long partitionId, TupleDomain<HyenaColumnHandle> predicate)
    {
        this.hyenaSession = requireNonNull(hyenaSession, "hyenaSession is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.partitionId = requireNonNull(partitionId, "partitionId is null");
        this.predicate = requireNonNull(predicate, "predicate is null");

        HyenaApi.ScanRequest req = new HyenaApi.ScanRequest();
        req.min_ts = 0;
        req.max_ts = Long.MAX_VALUE;
        req.partitionId = partitionId;
        req.projection = new ArrayList<>();
        req.filters = new ArrayList<>();
        for (HyenaColumnHandle col : columns) {
            req.projection.add(col.getOrdinalPosition());
        }

        if (predicate.getColumnDomains().isPresent()) {
            List<TupleDomain.ColumnDomain<HyenaColumnHandle>> handles = predicate.getColumnDomains().get();
            for (TupleDomain.ColumnDomain<HyenaColumnHandle> handle : handles) {
                Domain domain = handle.getDomain();
                HyenaColumnHandle column = handle.getColumn();

                Set<Object> values = domain.getValues().getValuesProcessor().transform(
                        ranges -> {
                            ImmutableSet.Builder<Object> columnValues = ImmutableSet.builder();
                            for (Range range : ranges.getOrderedRanges()) {
                                if (range.isSingleValue()) {
                                    Long val = (Long) range.getSingleValue();
                                    req.filters.add(new HyenaApi.ScanFilter(column.getOrdinalPosition(), HyenaApi.ScanComparison.Eq, val));
                                } else {
                                    if (range.getHigh().getValueBlock().isPresent()) {
                                        if (range.getHigh().getBound() == Marker.Bound.BELOW) {
                                            Long val = (Long) range.getHigh().getValue();
                                            req.filters.add(new HyenaApi.ScanFilter(column.getOrdinalPosition(), HyenaApi.ScanComparison.Lt, val));
                                        } else {
                                            throw new UnsupportedOperationException("We don't know how to handle this yet");
                                        }
                                    }
                                    if (range.getLow().getValueBlock().isPresent()) {
                                        if (range.getLow().getBound() == Marker.Bound.ABOVE) {
                                            Long val = (Long) range.getLow().getValue();
                                            req.filters.add(new HyenaApi.ScanFilter(column.getOrdinalPosition(), HyenaApi.ScanComparison.Gt, val));
                                        } else {
                                            throw new UnsupportedOperationException("We don't know how to handle this yet");
                                        }
                                    }

                                }

                            }
                            return columnValues.build();
                        },
                        discreteValues -> {
                            if (discreteValues.isWhiteList()) {
                                return ImmutableSet.copyOf(discreteValues.getValues());
                            }
                            return ImmutableSet.of();
                        },
                        allOrNone -> ImmutableSet.of());

                System.out.println(values);

            }
        }

        System.out.println(StringUtils.join(req.filters, ", "));

        this.result = this.hyenaSession.scan(req);
    }

    private void preparePredicates(TupleDomain<HyenaColumnHandle> predicate) {
        Optional<Map<HyenaColumnHandle, Domain>> domains = predicate.getDomains();
        if (!domains.isPresent()) {
            return;
        }
        // SUMTHIN
    }

    private HyenaApi.BlockHolder getBlockHolder(int col) {
        return this.result.blocks.get(col);
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
        return rowPosition++ < result.row_count;
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

        HyenaApi.BlockHolder holder = getBlockHolder(field);
        switch (holder.type) {
            case Int64Dense:
                return holder.int64DenseBlock.get(rowPosition);
            case Int64Sparse:
                return holder.int64SparseBlock.getMaybe(rowPosition);
            case Int32Sparse:
                return holder.int32SparseBlock.getMaybe(rowPosition);
        }
        throw new RuntimeException("Bad type");
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
        HyenaApi.BlockHolder holder = getBlockHolder(field);
        switch (holder.type) {
            case Int64Sparse:
                return holder.int64SparseBlock.getMaybe(rowPosition) == null;
            case Int32Sparse:
                return holder.int32SparseBlock.getMaybe(rowPosition) == null;
        }

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