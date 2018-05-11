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

import co.llective.hyena.api.ColumnValues;
import co.llective.hyena.api.ScanOrFilters;
import co.llective.hyena.api.ScanRequest;
import co.llective.hyena.api.ScanResult;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
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

    private long constructorStartMs;
    private long constructorFinishMs;
    private long scanStart;
    private long scanFinish;
    private long iteratingStartNs;

    public HyenaRecordCursor(HyenaSession hyenaSession, List<HyenaColumnHandle> columns, TupleDomain<HyenaColumnHandle> predicate)
    {
        this(new HyenaPredicatesUtil(), hyenaSession, columns, predicate);
    }

    public HyenaRecordCursor(HyenaPredicatesUtil predicateHandler, HyenaSession hyenaSession, List<HyenaColumnHandle> columns, TupleDomain<HyenaColumnHandle> predicate)
    {
        constructorStartMs = System.currentTimeMillis();
        this.columns = requireNonNull(columns, "columns is null");

        ScanRequest req = new ScanRequest();
        req.setProjection(new ArrayList<>());
        req.setFilters(new ScanOrFilters());

        for (HyenaColumnHandle col : columns) {
            req.getProjection().add(col.getOrdinalPosition());
        }

        Optional<AbstractMap.SimpleEntry<Long, Long>> tsBoundaries = predicateHandler.getTsConstraints(predicate);
        if (tsBoundaries.isPresent()) {
            req.setMinTs(tsBoundaries.get().getKey());
            req.setMaxTs(tsBoundaries.get().getValue());
        }
        else {
            req.setMinTs(0L);
            req.setMaxTs(Long.MAX_VALUE);
        }

        ScanOrFilters filters = predicateHandler.predicateToFilters(predicate);
        req.getFilters().addAll(filters);

        log.info("Filters: " + StringUtils.join(req.getFilters(), ", "));

        scanStart = System.currentTimeMillis();
        slicedResult = hyenaSession.scan(req);
        scanFinish = System.currentTimeMillis();
        rowCount = getRowCount(slicedResult);
        prepareSliceMappings();

        log.info("Received " + rowCount + " records");
        constructorFinishMs = System.currentTimeMillis();
    }

    private void prepareSliceMappings()
    {
        for (int field = 0; field < columns.size(); field++) {
            long columnId = columns.get(field).getOrdinalPosition();
            fieldsToColumns.put(field, this.slicedResult.getColumnMap().get(columnId));
        }
    }

    @VisibleForTesting
    int getRowCount(ScanResult slicedResult)
    {
        if (slicedResult.getColumnMap().isEmpty()) {
            return 0;
        }

        return slicedResult.getColumnMap().entrySet().stream()
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
            iteratingStartNs = System.nanoTime();
        }
        return ++rowPosition < rowCount;
    }

    @Override
    public boolean getBoolean(int field)
    {
        throw new NotImplementedException("Booleans are not supported yet");
    }

    @Override
    public long getLong(int field)
    {
        ColumnValues column = getColumn(field);

        switch (column.getType()) {
            case I128Dense:
            case U128Dense:
            case StringDense:
                throw new RuntimeException("Wrong type");
        }

        return column.getLong(rowPosition);
    }

    @Override
    public double getDouble(int field)
    {
        throw new NotImplementedException("Doubles are not supported yet");
    }

    @Override
    public Slice getSlice(int field)
    {
        return getColumn(field).getSlice(rowPosition);
    }

    @Override
    public Object getObject(int field)
    {
        throw new NotImplementedException("Objects are not supported yet");
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

    @Override
    public void close()
    {
        long closeTimeMs = System.currentTimeMillis();
        long iteratingTimeNs = (System.nanoTime() - iteratingStartNs);
        log.warn("Scan + deserialization time: " + (scanFinish - scanStart) + "ms");
        log.warn("Constructor time: " + (constructorFinishMs - constructorStartMs) + "ms");
        log.warn("Iterating time: " + (iteratingTimeNs / 1000000) + "ms");
        log.warn("Iterated through " + rowPosition + " rows, " + (rowPosition == 0 ? 0 : (iteratingTimeNs / rowPosition)) + "ns per row");
        log.warn("Whole cursor job: " + (closeTimeMs - constructorStartMs) + "ms");
        //TODO: cancel query in hyenaAPI (send abort request with requestID)
    }
}
