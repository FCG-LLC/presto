package co.llective.presto.hyena;

import co.llective.hyena.api.BlockHolder;
import co.llective.hyena.api.BlockType;
import co.llective.hyena.api.DataTriple;
import co.llective.hyena.api.DenseBlock;
import co.llective.hyena.api.ScanResult;
import co.llective.hyena.api.SparseBlock;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
            ScanResult scanResult = new ScanResult(Collections.emptyList());

            assertEquals(0, cursor.getRowCount(scanResult));
        }

        @Test
        public void returnsSizeOfDenseWhenExists()
        {
            int size = 10;
            DataTriple denseTriple = new DataTriple(0, BlockType.I16Dense, Optional.of(
                    new BlockHolder(BlockType.I16Dense, nElementDenseBlock(size, 1))));

            List<DataTriple> data = new ArrayList<>();
            data.add(denseTriple);
            ScanResult scanResult = new ScanResult(data);

            assertEquals(size, cursor.getRowCount(scanResult));
        }

        @Test
        public void returnsLastIndexOfSparseWhenNoDense()
        {
            int size = 14;
            DataTriple sparseTriple = new DataTriple(0, BlockType.I16Sparse, Optional.of(
                    new BlockHolder(BlockType.I16Sparse, nElementSparseBlock(size, 1))));

            List<DataTriple> data = new ArrayList<>();
            data.add(sparseTriple);
            ScanResult scanResult = new ScanResult(data);

            assertEquals(size, cursor.getRowCount(scanResult));
        }

        @Test
        public void returnsGreaterSizeWhenMoreThanOneSparseColumn()
        {
            int smallerSize = 5;
            int biggerSize = 10;
            DataTriple smallerSparseType = new DataTriple(0, BlockType.I16Sparse, Optional.of(
                    new BlockHolder(BlockType.I16Sparse, nElementSparseBlock(smallerSize, 1))));
            DataTriple biggerSparseType = new DataTriple(0, BlockType.I16Sparse, Optional.of(
                    new BlockHolder(BlockType.I16Sparse, nElementSparseBlock(biggerSize, 1))));

            List<DataTriple> data = new ArrayList<>();
            data.add(smallerSparseType);
            data.add(biggerSparseType);
            ScanResult scanResult = new ScanResult(data);

            assertEquals(biggerSize, cursor.getRowCount(scanResult));
        }

        @Test
        public void returnsDenseSizeWhenMixedColumns()
        {
            int denseSize = 10;
            int sparseSize = 5;
            DataTriple denseTriple = new DataTriple(0, BlockType.I16Dense, Optional.of(
                    new BlockHolder(BlockType.I16Dense, nElementDenseBlock(denseSize, 1))));
            DataTriple sparseTriple = new DataTriple(0, BlockType.I16Sparse, Optional.of(
                    new BlockHolder(BlockType.I16Sparse, nElementSparseBlock(sparseSize, 1))));

            List<DataTriple> data = new ArrayList<>();
            data.add(denseTriple);
            data.add(sparseTriple);
            ScanResult scanResult = new ScanResult(data);

            assertEquals(denseSize, cursor.getRowCount(scanResult));
        }

        private HyenaRecordCursor initHyenaRecordCursor()
        {
            HyenaSession hyenaSession = mock(HyenaSession.class);
            when(hyenaSession.scan(any())).thenReturn(new ScanResult(Collections.emptyList()));
            return new HyenaRecordCursor(hyenaSession, Collections.emptyList(), mock(HostAddress.class), TupleDomain.all());
        }

        <T extends Number> DenseBlock<T> nElementDenseBlock(int n, T defVal)
        {
            DenseBlock<T> block = new DenseBlock<>(BlockType.I16Dense, n);
            while (n > 0) {
                block.add(defVal);
                n--;
            }
            return block;
        }

        <T extends Number> SparseBlock<T> nElementSparseBlock(int n, T defVal)
        {
            SparseBlock<T> block = new SparseBlock<>(BlockType.I16Sparse, n);
            block.add(n, defVal);
            return block;
        }
    }
}
