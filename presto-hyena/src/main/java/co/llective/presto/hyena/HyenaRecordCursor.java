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
import co.llective.hyena.api.StreamConfig;
import co.llective.presto.hyena.util.TimeBoundaries;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
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
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class HyenaRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(HyenaRecordCursor.class);

    private final Long streamingLimit;
    private final Long streamingThreshold;
    private final Boolean streamingEnabled;

    private final List<HyenaColumnHandle> columns;
    private ScanResult slicedResult;
    private final HyenaSession hyenaSession;
    private ScanRequest scanRequest;
    @VisibleForTesting AtomicBoolean endOfScan = new AtomicBoolean(false);
    @VisibleForTesting int rowPosition = -1; // presto first advances next row and then fetch data
    @VisibleForTesting int rowCount;

    private Map<Integer, ColumnValues> fieldsToColumns = new HashMap<>();

    private long constructorStartMs;
    private long constructorFinishMs;
    private long iteratingStartNs;

    public HyenaRecordCursor(HyenaSession hyenaSession, ConnectorSession connectorSession, List<HyenaColumnHandle> columns, TupleDomain<HyenaColumnHandle> predicate, Optional<TimeBoundaries> timeBoundaries)
    {
        this(new HyenaPredicatesUtil(), hyenaSession, connectorSession, columns, predicate, timeBoundaries);
    }

    public HyenaRecordCursor(HyenaPredicatesUtil predicateHandler, HyenaSession hyenaSession, ConnectorSession connectorSession, List<HyenaColumnHandle> columns, TupleDomain<HyenaColumnHandle> predicate, Optional<TimeBoundaries> timeBoundaries)
    {
        constructorStartMs = System.currentTimeMillis();
        this.hyenaSession = hyenaSession;
        this.columns = requireNonNull(columns, "columns is null");
        this.streamingEnabled = HyenaConnectorSessionProperties.getStreamingEnabled(connectorSession);
        this.streamingLimit = HyenaConnectorSessionProperties.getStreamingRecordsLimit(connectorSession);
        this.streamingThreshold = HyenaConnectorSessionProperties.getStreamingRecordsThreshold(connectorSession);

        this.scanRequest = buildScanRequest(predicateHandler, columns, predicate, timeBoundaries);

        log.info("Filters: " + StringUtils.join(this.scanRequest.getFilters(), ", "));

        //TODO: Remove when hyena will fully support source_id
        remapSourceIdFilter(this.scanRequest);

        constructorFinishMs = System.currentTimeMillis();
    }

    private ScanRequest buildScanRequest(HyenaPredicatesUtil predicateHandler, List<HyenaColumnHandle> columns, TupleDomain<HyenaColumnHandle> predicate, Optional<TimeBoundaries> timeBoundaries)
    {
        ScanRequest req = new ScanRequest();
        req.setProjection(new ArrayList<>());
        req.setFilters(new ScanOrFilters());

        for (HyenaColumnHandle col : columns) {
            req.getProjection().add(col.getOrdinalPosition());
        }

        if (timeBoundaries.isPresent()) {
            req.setMinTs(timeBoundaries.get().getStart());
            req.setMaxTs(timeBoundaries.get().getEnd());
        }
        else {
            req.setMinTs(0L);
            req.setMaxTs(Long.MAX_VALUE);
        }

        ScanOrFilters filters = predicateHandler.predicateToFilters(predicate);
        req.getFilters().addAll(filters);

        if (streamingEnabled) {
            StreamConfig scanConfig = new StreamConfig(streamingLimit, streamingThreshold, Optional.empty());
            req.setScanConfig(Optional.of(scanConfig));
        }
        else {
            req.setScanConfig(Optional.empty());
        }

        return req;
    }

    /**
     * Fetches records from database.
     * If there are 0 records in next chunk it tries until there will be results or it is the end of the scan.
     */
    @VisibleForTesting void fetchRecordsFromDb()
    {
        do {
            long scanStart = System.currentTimeMillis();
            slicedResult = hyenaSession.scan(scanRequest);
            long scanFinish = System.currentTimeMillis();
            log.debug("Scan + deserialization time: " + (scanFinish - scanStart) + "ms");
            rowCount = getRowCount(slicedResult);
            log.debug("Received " + rowCount + " records");
            endOfScan.set(!slicedResult.getStreamState().isPresent());
            if (scanRequest.getScanConfig().isPresent() && slicedResult.getStreamState().isPresent()) {
                scanRequest.getScanConfig().get().setStreamState(slicedResult.getStreamState());
            }
        } while (rowCount == 0 && !endOfScan.get());
        prepareSliceMappings();
    }

    @VisibleForTesting void prepareSliceMappings()
    {
        for (int field = 0; field < columns.size(); field++) {
            long columnId = columns.get(field).getOrdinalPosition();
            fieldsToColumns.put(field, this.slicedResult.getColumnMap().get(columnId));
        }
    }

    private void remapSourceIdFilter(ScanRequest req)
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
                        new ScanFilter(0, ScanComparison.Gt, BlockType.U64Dense.mapToFilterType(), timestampIndex));
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
        if (++rowPosition < rowCount) {
            return true;
        }
        else {
            if (rowCount != 0) {
                long iteratingEndNs = System.nanoTime();
                log.debug("Iterated through " + rowPosition + " rows, " + (rowPosition == 0 ? 0 : ((iteratingEndNs - iteratingStartNs) / rowPosition)) + "ns per row");
            }
            if (endOfScan.get()) {
                log.debug("No more records in db. Finishing.");
                return false;
            }
            else {
                log.debug("Cursor needs more data. Scanning again.");
                fetchRecordsFromDb();
                rowPosition = 0;
                iteratingStartNs = System.nanoTime();
                return rowCount != 0;
            }
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        throw new NotImplementedException("Booleans are not supported yet");
    }

    @Override
    public long getLong(int field)
    {
        // TODO: temporal workaround for not filled source_id by hyena (we only have packet_headers now)
        if (columns.get(field).getColumnName().equals("source_id")) {
            return 1L;
        }

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
        log.debug("Constructor time: " + (constructorFinishMs - constructorStartMs) + "ms");
        log.debug("Whole cursor job: " + (closeTimeMs - constructorStartMs) + "ms");
        //TODO: cancel query in hyenaAPI (send abort request with requestID)
    }
}
