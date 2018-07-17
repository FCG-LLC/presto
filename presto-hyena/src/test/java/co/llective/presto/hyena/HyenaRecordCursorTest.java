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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.IntegerType;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class HyenaRecordCursorTest
{
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
            HyenaColumnHandle handle = new HyenaColumnHandle("", IntegerType.INTEGER, BlockType.I16Sparse, 0);
            return new HyenaRecordCursor(hyenaSession, connectorSession, Collections.singletonList(handle), TupleDomain.all());
        }
    }
}
