/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package co.llective.presto.hyena;

import co.llective.presto.hyena.api.HyenaApi;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HyenaRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(HyenaApi.class);

    private final List<HyenaColumnHandle> columns;
    private final Long partitionId;
    private final TupleDomain<HyenaColumnHandle> predicate;
    private final HyenaSession hyenaSession;
    private HyenaApi.HyenaOpMetadata hyenaOpMetadata;

    private List<String> fields;
    private int foobar;

    private final HyenaApi.ScanResult result;
    private int rowPosition;

    public HyenaRecordCursor(HyenaSession hyenaSession, List<HyenaColumnHandle> columns, HostAddress address, Long partitionId, TupleDomain<HyenaColumnHandle> predicate)
    {
        HyenaSession baseSession = requireNonNull(hyenaSession, "hyenaSession is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.partitionId = requireNonNull(partitionId, "partitionId is null");
        this.predicate = requireNonNull(predicate, "predicate is null");

        HyenaApi.ScanRequest req = new HyenaApi.ScanRequest();
        req.minTs = 0;
        req.maxTs = Long.MAX_VALUE;
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
                                    if (column.getColumnType() == VARCHAR) {
                                        Slice val = (Slice) range.getSingleValue();
                                        req.filters.add(new HyenaApi.ScanFilter(column.getOrdinalPosition(), HyenaApi.ScanComparison.Eq, 0, val.toStringUtf8()));
                                    }
                                    else {
                                        Long val = (Long) range.getSingleValue();
                                        req.filters.add(new HyenaApi.ScanFilter(column.getOrdinalPosition(), HyenaApi.ScanComparison.Eq, val, ""));
                                    }
                                }
                                else {
                                    if (range.getHigh().getValueBlock().isPresent()) {
                                        Marker high = range.getHigh();

                                        HyenaApi.ScanFilterBuilder builder = new HyenaApi.ScanFilterBuilder().withColumn(column.getOrdinalPosition());

                                        if (high.getBound() == Marker.Bound.BELOW) {
                                            builder.withOp(HyenaApi.ScanComparison.Lt);
                                        }
                                        else if (high.getBound() == Marker.Bound.EXACTLY) {
                                            builder.withOp(HyenaApi.ScanComparison.LtEq);
                                        }
                                        else {
                                            throw new UnsupportedOperationException("We don't know how to handle this yet - high values and neither below nor exactly marker present?");
                                        }

                                        if (column.getColumnType() == VARCHAR) {
                                            builder.withStringValue(((Slice) high.getValue()).toStringUtf8());
                                        }
                                        else {
                                            builder.withLongValue((Long) high.getValue());
                                        }

                                        req.filters.add(builder.build());
                                    }

                                    if (range.getLow().getValueBlock().isPresent()) {
                                        HyenaApi.ScanFilterBuilder builder = new HyenaApi.ScanFilterBuilder().withColumn(column.getOrdinalPosition());

                                        Marker low = range.getLow();

                                        if (low.getBound() == Marker.Bound.ABOVE) {
                                            builder.withOp(HyenaApi.ScanComparison.Gt);
                                        }
                                        else if (low.getBound() == Marker.Bound.EXACTLY) {
                                            builder.withOp(HyenaApi.ScanComparison.GtEq);
                                        }
                                        else {
                                            throw new UnsupportedOperationException("We don't know how to handle this yet - low values and neither above nor exactly marker present?");
                                        }

                                        if (column.getColumnType() == VARCHAR) {
                                            builder.withStringValue(((Slice) low.getValue()).toStringUtf8());
                                        }
                                        else {
                                            builder.withLongValue((Long) low.getValue());
                                        }

                                        req.filters.add(builder.build());
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
            }
        }

        log.info("Filters: " + StringUtils.join(req.filters, ", "));

        this.hyenaSession = baseSession.recordSetProviderSession();
        hyenaOpMetadata = new HyenaApi.HyenaOpMetadata();
        result = this.hyenaSession.scan(req, hyenaOpMetadata);
    }

    private void preparePredicates(TupleDomain<HyenaColumnHandle> predicate)
    {
        Optional<Map<HyenaColumnHandle, Domain>> domains = predicate.getDomains();
        if (!domains.isPresent()) {
            return;
        }
        // SUMTHIN
    }

    private HyenaApi.BlockHolder getBlockHolder(int col)
    {
        return this.result.blocks.get(col);
    }

    @Override
    public long getCompletedBytes()
    {
        return this.hyenaOpMetadata.bytes;
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
        return ++rowPosition < result.rowCount;
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
            case Int16Sparse:
                return holder.int16SparseBlock.getMaybe(rowPosition);
            case Int8Sparse:
                return holder.int8SparseBlock.getMaybe(rowPosition);
        }
        throw new RuntimeException("Bad type");
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(int field)
    {
        HyenaApi.BlockHolder holder = getBlockHolder(field);
        if (holder.type != HyenaApi.BlockType.String) {
            throw new RuntimeException("Expected String block type. Found " + holder.type.name() + " instead");
        }

        return utf8Slice(holder.stringBlock.getMaybe(rowPosition));
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
//            case Int64Dense:
//                return holder.int64DenseBlock.data.size() >= rowPosition;
            case Int64Sparse:
                return holder.int64SparseBlock.getMaybe(rowPosition) == null;
            case Int32Sparse:
                return holder.int32SparseBlock.getMaybe(rowPosition) == null;
            case Int16Sparse:
                return holder.int16SparseBlock.getMaybe(rowPosition) == null;
            case Int8Sparse:
                return holder.int8SparseBlock.getMaybe(rowPosition) == null;
            case String:
                return holder.stringBlock.getMaybe(rowPosition) == null;
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
        this.hyenaSession.close();
    }
}
