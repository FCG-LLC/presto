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
import co.llective.hyena.api.DenseNumberColumn;
import co.llective.hyena.api.ScanResult;
import co.llective.hyena.api.SparseNumberColumn;
import co.llective.hyena.api.StreamState;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.IntegerType;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class HyenaRecordCursorTest
{
    public static class FetchRecordsFromDb
    {
        HyenaSession session;
        ConnectorSession connectorSession;
        HyenaRecordCursor cursor;
        ScanResult scanResult;

        HyenaColumnHandle column = new HyenaColumnHandle("colName", IntegerType.INTEGER, BlockType.I32Dense, 0);

        @BeforeMethod
        public void setUp()
        {
            session = mock(HyenaSession.class);
            connectorSession = mock(ConnectorSession.class);
            when(connectorSession.getProperty(any(), any())).thenReturn(10L);
            cursor = spy(new HyenaRecordCursor(session, connectorSession, Collections.singletonList(column), TupleDomain.all()));
            doNothing().when(cursor).prepareSliceMappings();
        }

        @Test
        public void setsEndOfScanWhenNoStreamState()
        {
            scanResult = new ScanResult(Collections.emptyMap(), Optional.empty());
            doReturn(10).when(cursor).getRowCount(any());
            doReturn(scanResult).when(session).scan(any());

            cursor.endOfScan.set(false);

            cursor.fetchRecordsFromDb();

            verify(session).scan(any());
            assertTrue(cursor.endOfScan.get());
        }

        @Test
        public void scansUntilResultsReturned()
        {
            scanResult = new ScanResult(Collections.emptyMap(), Optional.of(new StreamState(10)));
            doReturn(scanResult).when(session).scan(any());

            doReturn(0).doReturn(0).doReturn(10)
                    .when(cursor)
                    .getRowCount(any());

            cursor.fetchRecordsFromDb();

            verify(session, times(3)).scan(any());
            assertFalse(cursor.endOfScan.get());
        }

        @Test
        public void scansUntilNoStreamState()
        {
            scanResult = new ScanResult(Collections.emptyMap(), Optional.of(new StreamState(10)));
            ScanResult scanResultWithNoStreamState = new ScanResult(Collections.emptyMap(), Optional.empty());

            doReturn(scanResult).doReturn(scanResultWithNoStreamState).when(session).scan(any());

            doReturn(0).doReturn(10)
                    .when(cursor)
                    .getRowCount(any());

            cursor.fetchRecordsFromDb();

            verify(session, times(2)).scan(any());
            assertTrue(cursor.endOfScan.get());
        }

        @Test
        public void setsRowCountAfterScan()
        {
            cursor.rowCount = 1;

            scanResult = new ScanResult(Collections.emptyMap(), Optional.of(new StreamState(10)));

            doReturn(scanResult).when(session).scan(any());

            doReturn(10)
                    .when(cursor)
                    .getRowCount(scanResult);

            cursor.fetchRecordsFromDb();

            verify(session).scan(any());
            assertEquals(cursor.rowCount, 10);
        }

        @Test
        public void preparesSliceMappings()
        {
            scanResult = new ScanResult(Collections.emptyMap(), Optional.of(new StreamState(10)));

            doReturn(scanResult).when(session).scan(any());

            doReturn(10)
                    .when(cursor)
                    .getRowCount(scanResult);

            cursor.fetchRecordsFromDb();

            verify(cursor).prepareSliceMappings();
        }
    }

    public static class AdvanceNextPosition
    {
        HyenaSession session;
        ConnectorSession connectorSession;
        HyenaRecordCursor cursor;

        HyenaColumnHandle column = new HyenaColumnHandle("colName", IntegerType.INTEGER, BlockType.I32Dense, 0);

        @BeforeMethod
        public void setUp()
        {
            session = mock(HyenaSession.class);
            connectorSession = mock(ConnectorSession.class);
            when(connectorSession.getProperty(any(), any())).thenReturn(10L);
            cursor = spy(new HyenaRecordCursor(session, connectorSession, Collections.singletonList(column), TupleDomain.all()));
        }

        @Test
        public void scansWhenFirstRun()
        {
            ScanResult scanResult = new ScanResult(Collections.emptyMap(), Optional.empty());

            doReturn(scanResult).when(session).scan(any());
            doReturn(10).when(cursor).getRowCount(any());
            doNothing().when(cursor).prepareSliceMappings();

            cursor.advanceNextPosition();
            verify(session).scan(any());
        }

        @Test
        public void trueWhenMoreRecordsToIterateInCurrentScan()
        {
            cursor.rowPosition = 1;
            cursor.rowCount = 10;

            assertTrue(cursor.advanceNextPosition());
        }

        @Test
        public void falseWhenFinishedScanningAndIterating()
        {
            cursor.rowPosition = 9;
            cursor.rowCount = 10;
            cursor.endOfScan.set(true);

            assertFalse(cursor.advanceNextPosition());
            verify(session, never()).scan(any());
        }

        @Test
        public void trueWhenNextScanReturnedResults()
        {
            cursor.rowPosition = 9;
            cursor.rowCount = 10;
            cursor.endOfScan.set(false);

            ScanResult scanResult = new ScanResult(Collections.emptyMap(), Optional.empty());

            when(session.scan(any())).thenReturn(scanResult);
            doReturn(5).when(cursor).getRowCount(scanResult);
            doNothing().when(cursor).prepareSliceMappings();

            assertTrue(cursor.advanceNextPosition());
            verify(session).scan(any());
        }

        @Test
        public void falseWhenLastScanReturnedZeroRows()
        {
            cursor.rowPosition = 9;
            cursor.rowCount = 10;
            cursor.endOfScan.set(false);

            ScanResult scanResult = new ScanResult(Collections.emptyMap(), Optional.empty());

            doReturn(scanResult).when(session).scan(any());
            doReturn(0).when(cursor).getRowCount(scanResult);
            doNothing().when(cursor).prepareSliceMappings();

            assertFalse(cursor.advanceNextPosition());
            verify(session).scan(any());
        }
    }

    public static class GetRowCount
    {
        HyenaRecordCursor cursor;

        @BeforeTest
        public void setUpCursor()
        {
            cursor = initHyenaRecordCursor();
        }

        @Test
        public void returnsZeroIfEmptyResult()
        {
            ScanResult scanResult = new ScanResult(new HashMap<>(), Optional.empty());

            assertEquals(0, cursor.getRowCount(scanResult));
        }

        @Test
        public void returnsSizeOfDenseWhenExists()
        {
            int size = 10;

            ColumnValues column = new DenseNumberColumn(BlockType.I16Dense, Slices.EMPTY_SLICE, size);

            Map<Long, ColumnValues> data = new HashMap<>();
            data.put(0L, column);
            ScanResult scanResult = new ScanResult(data, Optional.empty());

            assertEquals(size, cursor.getRowCount(scanResult));
        }

        @Test
        public void returnsSizeOfSparseWhenNoDense()
        {
            int size = 14;

            ColumnValues column = new SparseNumberColumn(BlockType.I16Sparse, Slices.EMPTY_SLICE, Slices.EMPTY_SLICE, size);
            Map<Long, ColumnValues> data = new HashMap<>();
            data.put(0L, column);
            ScanResult scanResult = new ScanResult(data, Optional.empty());

            assertEquals(size, cursor.getRowCount(scanResult));
        }

        @Test
        public void returnsGreaterSizeWhenMoreThanOneSparseColumn()
        {
            int smallerSize = 5;
            int greaterSize = 10;

            ColumnValues smallerSparse = new SparseNumberColumn(BlockType.I16Sparse, Slices.EMPTY_SLICE, Slices.EMPTY_SLICE, smallerSize);
            ColumnValues greaterSparse = new SparseNumberColumn(BlockType.I16Sparse, Slices.EMPTY_SLICE, Slices.EMPTY_SLICE, greaterSize);

            Map<Long, ColumnValues> data = new HashMap<>();
            data.put(0L, smallerSparse);
            data.put(1L, greaterSparse);

            ScanResult scanResult = new ScanResult(data, Optional.empty());

            assertEquals(greaterSize, cursor.getRowCount(scanResult));
        }

        @Test
        public void returnsDenseSizeWhenMixedColumns()
        {
            int denseSize = 10;
            int sparseSize = 5;

            ColumnValues sparse = new SparseNumberColumn(BlockType.I16Sparse, Slices.EMPTY_SLICE, Slices.EMPTY_SLICE, sparseSize);
            ColumnValues dense = new DenseNumberColumn(BlockType.I16Sparse, Slices.EMPTY_SLICE, denseSize);

            Map<Long, ColumnValues> data = new HashMap<>();
            data.put(0L, sparse);
            data.put(1L, dense);

            ScanResult scanResult = new ScanResult(data, Optional.empty());

            assertEquals(denseSize, cursor.getRowCount(scanResult));
        }

        private HyenaRecordCursor initHyenaRecordCursor()
        {
            HyenaSession hyenaSession = mock(HyenaSession.class);
            when(hyenaSession.scan(any())).thenReturn(new ScanResult(new HashMap<>(), Optional.empty()));
            ConnectorSession connectorSession = mock(ConnectorSession.class);
            when(connectorSession.getProperty(any(), any())).thenReturn(10L);
            HyenaColumnHandle handle = new HyenaColumnHandle("", IntegerType.INTEGER, BlockType.I16Sparse, 0);
            return new HyenaRecordCursor(hyenaSession, connectorSession, Collections.singletonList(handle), TupleDomain.all());
        }
    }
}
