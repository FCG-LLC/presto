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

import co.llective.hyena.api.Block;
import co.llective.hyena.api.BlockHolder;
import co.llective.hyena.api.BlockType;
import co.llective.hyena.api.DataTriple;
import co.llective.hyena.api.DenseBlock;
import co.llective.hyena.api.FilterType;
import co.llective.hyena.api.ScanComparison;
import co.llective.hyena.api.ScanFilter;
import co.llective.hyena.api.ScanFilterBuilder;
import co.llective.hyena.api.ScanRequest;
import co.llective.hyena.api.ScanResult;
import co.llective.hyena.api.SparseBlock;
import co.llective.hyena.api.StringBlock;
import co.llective.presto.hyena.types.U64Type;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.UnsignedLong;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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
    private static final Logger log = Logger.get(HyenaRecordCursor.class);

    private final List<HyenaColumnHandle> columns;
    private final HyenaSession hyenaSession;

    private final ScanResult result;
    private int rowPosition = -1; // presto first advances next row and then fetch data
    private final int rowCount;

    public HyenaRecordCursor(HyenaSession hyenaSession, List<HyenaColumnHandle> columns, HostAddress address, TupleDomain<HyenaColumnHandle> predicate)
    {
        this.hyenaSession = requireNonNull(hyenaSession, "hyenaSession is null");
        this.columns = requireNonNull(columns, "columns is null");

        ScanRequest req = new ScanRequest();
        req.setMinTs(0);
        req.setMaxTs(Long.MAX_VALUE);
        req.setProjection(new ArrayList<>());
        req.setFilters(new ArrayList<>());

        for (HyenaColumnHandle col : columns) {
            req.getProjection().add(col.getOrdinalPosition());
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
                                        req.getFilters().add(new ScanFilter(column.getOrdinalPosition(), ScanComparison.Eq, FilterType.String, val, Optional.of(val.toStringUtf8())));
                                    }
                                    else {
                                        Long val = (Long) range.getSingleValue();
                                        req.getFilters().add(new ScanFilter(column.getOrdinalPosition(), ScanComparison.Eq, column.getHyenaType().mapToFilterType(), val, Optional.of("")));
                                    }
                                }
                                else {
                                    if (range.getHigh().getValueBlock().isPresent()) {
                                        Marker high = range.getHigh();

                                        ScanFilterBuilder builder = new ScanFilterBuilder(hyenaSession.refreshCatalog()).withColumn(column.getOrdinalPosition());

                                        if (high.getBound() == Marker.Bound.BELOW) {
                                            builder = builder.withOp(ScanComparison.Lt);
                                        }
                                        else if (high.getBound() == Marker.Bound.EXACTLY) {
                                            builder = builder.withOp(ScanComparison.LtEq);
                                        }
                                        else {
                                            throw new UnsupportedOperationException("We don't know how to handle this yet - high values and neither below nor exactly marker present?");
                                        }

                                        if (column.getColumnType() == VARCHAR) {
                                            builder = builder.withStringValue(((Slice) high.getValue()).toStringUtf8());
                                        }
                                        else if (column.getColumnType() == U64Type.U_64_TYPE) {
                                            //TODO: replace with proper handling of U64 filters
                                            Long value = (Long) high.getValue();
                                            if (value < 0) {
                                                builder = builder.withOp(ScanComparison.Gt);
                                                builder = builder.withValue(UnsignedLong.MAX_VALUE.longValue());
                                            }
                                            else {
                                                builder = builder.withValue(value);
                                            }
                                        }
                                        else {
                                            builder = builder.withValue((Long) high.getValue());
                                        }

                                        req.getFilters().add(builder.build());
                                    }

                                    if (range.getLow().getValueBlock().isPresent()) {
                                        ScanFilterBuilder builder = new ScanFilterBuilder(hyenaSession.refreshCatalog()).withColumn(column.getOrdinalPosition());

                                        Marker low = range.getLow();

                                        if (low.getBound() == Marker.Bound.ABOVE) {
                                            builder = builder.withOp(ScanComparison.Gt);
                                        }
                                        else if (low.getBound() == Marker.Bound.EXACTLY) {
                                            builder = builder.withOp(ScanComparison.GtEq);
                                        }
                                        else {
                                            throw new UnsupportedOperationException("We don't know how to handle this yet - low values and neither above nor exactly marker present?");
                                        }

                                        if (column.getColumnType() == VARCHAR) {
                                            builder = builder.withStringValue(((Slice) low.getValue()).toStringUtf8());
                                        }
                                        else if (column.getColumnType() == U64Type.U_64_TYPE) {
                                            //TODO: replace with proper handling of U64 filters
                                            Long value = (Long) low.getValue();
                                            if (value < 0) {
                                                builder = builder.withOp(ScanComparison.GtEq);
                                                builder = builder.withValue(UnsignedLong.ZERO.longValue());
                                            }
                                            else {
                                                builder = builder.withValue(value);
                                            }
                                        }
                                        else {
                                            builder = builder.withValue((Long) low.getValue());
                                        }

                                        req.getFilters().add(builder.build());
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

        log.info("Filters: " + StringUtils.join(req.getFilters(), ", "));

        result = this.hyenaSession.scan(req);

        rowCount = getRowCount(result);
    }

    int getRowCount(ScanResult result)
    {
        if (result.getData().isEmpty()) {
            return 0;
        }

        Optional<Long> sourceIdPosition = columns.stream().filter(x -> x.getColumnName().equals("source_id")).findFirst().map(
                HyenaColumnHandle::getOrdinalPosition);

        return result.getData().stream()
                //TODO: Hyena doesn't support source_id column properly yet
                .filter(triple -> {
                    if (sourceIdPosition.isPresent()) {
                        return triple.getColumnId() != sourceIdPosition.get();
                    }
                    return true;
                })
                //if dense block
                .filter(x -> x.getColumnType().isDense())
                .map(dt -> dt.getData().map(bh -> bh.getBlock().count()).orElse(0))
                .findFirst()
                // if no dense blocks then check sparse ones
                .orElseGet(() ->
                        result.getData().stream()
                                //TODO: Hyena doesn't support source_id column properly yet
                                .filter(triple -> {
                                    if (sourceIdPosition.isPresent()) {
                                        return triple.getColumnId() != sourceIdPosition.get();
                                    }
                                    return true;
                                })
                                .map(dt -> dt.getData().map(bh -> {
                                    List<Integer> offsets = ((SparseBlock) bh.getBlock()).getOffsetData();
                                    // last element of offset shows last non-null element of row
                                    return offsets.get(offsets.size() - 1);
                                }).orElse(0))
                                // we're taking max count of all columns
                                .max(Comparator.naturalOrder()).orElse(0));
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
        return ++rowPosition < rowCount;
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
        checkFieldType(field, BIGINT, INTEGER, U64Type.U_64_TYPE);

        // temporal workaround for not filled source_id by hyena (we only have packet_headers now)
        if (columns.get(field).getColumnName().equals("source_id")) {
            return 1L;
        }

        BlockHolder holder = getBlockHolderOrThrow(field);
        Block block = holder.getBlock();
        switch (holder.getType()) {
            case I8Dense:
                return ((DenseBlock<Byte>) block).getData().get(rowPosition).longValue();
            case I16Dense:
            case U8Dense:
                return ((DenseBlock<Short>) block).getData().get(rowPosition).longValue();
            case I32Dense:
            case U16Dense:
                return ((DenseBlock<Integer>) block).getData().get(rowPosition).longValue();
            case I64Dense:
            case U32Dense:
                return ((DenseBlock<Long>) block).getData().get(rowPosition);
            case U64Dense:
                return ((DenseBlock<BigInteger>) block).getData().get(rowPosition).longValue();
            case I8Sparse:
                return ((SparseBlock<Byte>) block).getMaybe(rowPosition).get().longValue();
            case I16Sparse:
            case U8Sparse:
                return ((SparseBlock<Short>) block).getMaybe(rowPosition).get().longValue();
            case I32Sparse:
            case U16Sparse:
                return ((SparseBlock<Integer>) block).getMaybe(rowPosition).get().longValue();
            case I64Sparse:
            case U32Sparse:
                return ((SparseBlock<Long>) block).getMaybe(rowPosition).get();
            case U64Sparse:
                return ((SparseBlock<BigInteger>) block).getMaybe(rowPosition).get().longValue();
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
        BlockHolder holder = getBlockHolderOrThrow(field);

        if (holder.getType() != BlockType.String) {
            throw new RuntimeException("Expected String block type. Found " + holder.getType().name() + " instead");
        }

        StringBlock block = (StringBlock) holder.getBlock();
        Optional<String> value = block.getMaybe(rowPosition);
        if (value.isPresent()) {
            return utf8Slice(block.getMaybe(rowPosition).get());
        }
        else {
            return null;
        }
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        BlockHolder holder = getBlockHolderOrThrow(field);

        switch (holder.getType()) {
            case U64Sparse:
            case U32Sparse:
            case U16Sparse:
            case U8Sparse:
            case I64Sparse:
            case I32Sparse:
            case I16Sparse:
            case I8Sparse:
                SparseBlock sparseBlock = (SparseBlock) holder.getBlock();
                return !sparseBlock.getMaybe(rowPosition).isPresent();
            case String:
                StringBlock stringBlock = (StringBlock) holder.getBlock();
                return !stringBlock.getMaybe(rowPosition).isPresent();
        }

        return false;
    }

    private BlockHolder getBlockHolderOrThrow(int field)
    {
        long columnId = columns.get(field).getOrdinalPosition();
        Optional<BlockHolder> optionalHolder = this.result.getData().stream()
                .filter(triple -> triple.getColumnId() == columnId)
                .map(DataTriple::getData)
                .findFirst().orElse(Optional.empty());

        if (!optionalHolder.isPresent()) {
            throw new RuntimeException("Empty block holder");
        }

        return optionalHolder.get();
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
        //TODO: cancel query in hyenaAPI (send abort request with requestID)
    }
}
