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

import co.llective.hyena.api.BlockType;
import co.llective.hyena.api.ColumnValues;
import co.llective.hyena.api.ScanAndFilters;
import co.llective.hyena.api.ScanComparison;
import co.llective.hyena.api.ScanFilter;
import co.llective.hyena.api.ScanOrFilters;
import co.llective.hyena.api.ScanRequest;
import co.llective.hyena.api.ScanResult;
import co.llective.presto.hyena.types.U64Type;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HyenaRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(HyenaRecordCursor.class);

    private final List<HyenaColumnHandle> columns;
    private final ScanResult slicedResult;
    private int rowPosition = -1; // presto first advances next row and then fetch data
    private final int rowCount;

    private Map<Integer, ColumnValues> fieldsToColumns = new HashMap<>();

    private long constructorStart;
    private long constructorFinish;
    private long scanStart;
    private long scanFinish;
    private long iteratingStart;

    public HyenaRecordCursor(HyenaSession hyenaSession, List<HyenaColumnHandle> columns, TupleDomain<HyenaColumnHandle> predicate)
    {
        this(new HyenaPredicatesUtil(), hyenaSession, columns, predicate);
    }

    public HyenaRecordCursor(HyenaPredicatesUtil predicateHandler, HyenaSession hyenaSession, List<HyenaColumnHandle> columns, TupleDomain<HyenaColumnHandle> predicate)
    {
        constructorStart = System.currentTimeMillis();
        this.columns = requireNonNull(columns, "columns is null");

        ScanRequest req = new ScanRequest();
        req.setMinTs(0);
        req.setMaxTs(Long.MAX_VALUE);
        req.setProjection(new ArrayList<>());
        req.setFilters(new ScanOrFilters());

        for (HyenaColumnHandle col : columns) {
            req.getProjection().add(col.getOrdinalPosition());
        }

        ScanOrFilters filters = predicateHandler.predicateToFilters(predicate);
        req.getFilters().addAll(filters);

        log.info("Filters: " + StringUtils.join(req.getFilters(), ", "));

        //TODO: Remove when hyena will fully support source_id
        remapSourceIdFilter(req);

        scanStart = System.currentTimeMillis();
        slicedResult = hyenaSession.scan(req);
        scanFinish = System.currentTimeMillis();
        rowCount = getRowCount(slicedResult);
        prepareSliceMappings();

        log.info("Received " + rowCount + " records");
        constructorFinish = System.currentTimeMillis();
    }

    private void prepareSliceMappings()
    {
        for (int field = 0; field < columns.size(); field++) {
            long columnId = columns.get(field).getOrdinalPosition();
            fieldsToColumns.put(field, this.slicedResult.getColumnMap().get(columnId));
        }
    }

    void remapSourceIdFilter(ScanRequest req)
    {
        Optional<Long> sourceIdPosition = columns.stream()
                .filter(x -> x.getColumnName().equals("source_id"))
                .findFirst()
                .map(HyenaColumnHandle::getOrdinalPosition);

        // if there isn't source_id in this query context it won't be in filters
        if (!sourceIdPosition.isPresent()) {
            return;
        }

        // if there is no or filters
        if (req.getFilters().isEmpty()) {
            return;
        }

        // if there are or filters but all are empty
        boolean areAndFilters = false;
        for (ScanAndFilters andFilters : req.getFilters()) {
            if (!andFilters.isEmpty()) {
                areAndFilters = true;
            }
        }
        if (!areAndFilters) {
            return;
        }

        // if there are filters but none of them is source_id one
        if (req.getFilters().stream().allMatch(andFilters ->
                andFilters.stream().allMatch(filter ->
                        filter.getColumn() != sourceIdPosition.get()))) {
            return;
        }

        for (ScanAndFilters andFilters : req.getFilters()) {
            if (andFilters.size() == 1) {
                andFilters.removeIf(x -> x.getColumn() == sourceIdPosition.get());
                // add tautology filter, u64 > 0
                long timestampIndex = 0L;
                if (!req.getProjection().contains(timestampIndex)) {
                    req.getProjection().add(timestampIndex);
                }
                andFilters.add(
                        new ScanFilter(0, ScanComparison.Gt, BlockType.U64Dense.mapToFilterType(), timestampIndex, Optional.empty()));
            }
            else {
                andFilters.removeIf(x -> x.getColumn() == sourceIdPosition.get());
            }
        }
    }

    @VisibleForTesting
    int getRowCount(ScanResult slicedResult)
    {
        if (slicedResult.getColumnMap().isEmpty()) {
            return 0;
        }

        //TODO: source_id removal
        Optional<Long> sourceIdPosition = columns.stream().filter(x -> x.getColumnName().equals("source_id")).findFirst().map(
                HyenaColumnHandle::getOrdinalPosition);

        return slicedResult.getColumnMap().entrySet().stream()
                .filter(entry -> sourceIdPosition.map(sourceIdCol -> !entry.getKey().equals(sourceIdCol)).orElse(true))
                .map(x -> x.getValue().getElementsCount())
                .max(Comparator.naturalOrder())
                .orElse(0);
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
        if (rowPosition == -1) {
            iteratingStart = System.currentTimeMillis();
        }
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

        // TODO: temporal workaround for not filled source_id by hyena (we only have packet_headers now)
        if (columns.get(field).getColumnName().equals("source_id")) {
            return 1L;
        }

        ColumnValues column = getColumn(field);

        switch(column.getType()) {
            case I128Dense:
            case U128Dense:
            case String:
                throw new RuntimeException("Wrong type");
        }

        return column.getLong(rowPosition);
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
        throw new NotImplementedException("Strings not implemented yet");
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        return getColumn(field).isNull(rowPosition);
    }

    private ColumnValues getColumn(int field)
    {
        ColumnValues column = fieldsToColumns.get(field);
        if (column == null) {
            throw new RuntimeException("Empty block holder");
        }
        return column;
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
        long closeTime = System.currentTimeMillis();
        long iteratingTime = (closeTime - iteratingStart);
        log.warn("Scan + deserialization time: " + (scanFinish - scanStart) + "ms");
        log.warn("Constructor time: " + (constructorFinish - constructorStart) + "ms");
        log.warn("Iterating time: " + (closeTime - iteratingStart) + "ms");
        log.warn("Iterated through " + rowPosition + " rows, " + (iteratingTime / rowPosition) + "ms per row");
        log.warn("Whole cursor job: " + (closeTime - constructorStart) + "ms");
        //TODO: cancel query in hyenaAPI (send abort request with requestID)
    }
}
