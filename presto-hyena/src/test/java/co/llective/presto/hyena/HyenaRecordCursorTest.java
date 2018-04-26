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

import co.llective.hyena.api.BlockHolder;
import co.llective.hyena.api.BlockType;
import co.llective.hyena.api.DataTriple;
import co.llective.hyena.api.DenseBlock;
import co.llective.hyena.api.ScanResult;
import co.llective.hyena.api.SparseBlock;
import co.llective.hyena.api.Testtt;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.IntegerType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.commons.collections.FastHashMap;
import org.clapper.util.misc.SparseArrayList;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class HyenaRecordCursorTest
{
    @Test
    public void hashMapTest() {
        int size = 10000000;
        HashMap<Integer, Integer> map = new HashMap<>(size, 1);
        long putMapTime = measureTimeMs(() ->
        {
            for (int i = 0; i < size; i++) {
                map.put(i, i);
            }
        });
        TreeMap<Integer, Integer> treeMap = new TreeMap<>();
        long putTreeMapTime = measureTimeMs(() ->
        {
            for (int i = 0; i < size; i++) {
                treeMap.put(i, i);
            }
        });
        List<Integer> arrayList = new LinkedList<>();
        long putListTime = measureTimeMs(() -> {
            for (int i = 0; i < size; i++) {
                arrayList.add(i);
            }
        });
        int[] array = new int[size];
        long putArrayTime = measureTimeMs(() -> {
            for (int i = 0; i < size; i++) {
                array[i] = i;
            }
        });
        long readMapTime = measureTimeMs(() -> {
            for (int i = 0; i < size; i++) {
                map.get(i);
            }
        });
        FastHashMap fastHashMap = new FastHashMap();
        long fastHMPutTime = measureTimeMs(() -> {
            for (int i = 0; i < size; i++) {
                fastHashMap.put(i, i);
            }
        });
        SparseArrayList<Integer> sparseArrayList = new SparseArrayList<>(size);
        long putSparseListTime = measureTimeMs(() -> {
            for (int i = 0; i < size; i++) {
                sparseArrayList.add(i);
            }
        });

//        long readSparseArrayList = measureTimeMs(() -> {
//            for (int i = 0; i < size; i++) {
//                sparseArrayList.indexOf(i);
//            }
//        });
        System.out.println("Putting into HashMap " + size + " elements took " + putMapTime + "ms");
        System.out.println("Putting into FastHashMap " + size + " elements took " + fastHMPutTime + "ms");
        System.out.println("Putting into ArrayList " + size + " elements took " + putListTime + "ms");
        System.out.println("Putting into SparseArrayList " + size + " elements took " + putSparseListTime + "ms");
        System.out.println("Putting into TreeMap " + size + " elements took " + putTreeMapTime + "ms");
        System.out.println("Putting into array " + size + " elements took " + putArrayTime + "ms");
        System.out.println("Getting " + size + " elements from HashMap took " + readMapTime + "ms");
//        System.out.println("Getting " + size + " elements from FastArrayList took " + readSparseArrayList + "ms");
    }

    @Test
    public void kotlinTest() {
        Testtt kotlinTest = new Testtt();
        JavaTesttt javaTest = new JavaTesttt();
        long iterations = 0;
        long javaSumTime = 0;
        long kotlinSumTime = 0;
        long emptySumTime = 0;
        do {
            long startT = System.nanoTime();
            javaTest.imDoingNothing();
            long time = System.nanoTime() - startT;
            javaSumTime += time;

            startT = System.nanoTime();
            kotlinTest.imDoingNothing();
            time = System.nanoTime() - startT;
            kotlinSumTime += time;

            startT = System.nanoTime();
            time = System.nanoTime() - startT;
            emptySumTime += time;

            iterations++;
        } while (iterations < 100000000);
        System.out.println("Java sum time: " + javaSumTime + "ns, avg: " + (javaSumTime/ (iterations + 1)) + "ns");
        System.out.println("Kotlin sum time: " + kotlinSumTime + "ns, avg: " + (kotlinSumTime/ (iterations + 1)) + "ns");
        System.out.println("No method sum time: " + emptySumTime + "ns, avg: " + (emptySumTime/ (iterations + 1)) + "ns");
    }

    @Test
    public void javaTest() {
        JavaTesttt test = new JavaTesttt();
        long iterations = 0;
        long sumTime = 0;
        do {
            long startT = System.nanoTime();
            test.imDoingNothing();
            long time = System.nanoTime() - startT;
            sumTime += time;
            iterations++;
        } while (iterations < 100000);
        System.out.println("Sum time: " + sumTime + "ns, avg: " + (sumTime/ (iterations + 1)) + "ns");
    }

    @Test
    public void test() {
        Slice slice = Slices.utf8Slice("dupa1211111111111113");
//        measureTime(() -> slice.getByte(0));
//        long getInt = measureTime(() -> slice.getInt(0));
//        long getLong = measureTime(() -> slice.getLong(0));
        long castInt = measureTimeNs(() -> {
            long a = (long) slice.getInt(0);
        });
        long castInt2 = measureTimeNs(() -> {
            long a = (long) slice.getInt(8);
        });

//        System.out.println("getting int: " + getInt + " nanos");
//        System.out.println("getting long: " + getLong + " nanos");
        System.out.println("getting int and casting to long: " + castInt + " nanos");
        System.out.println("getting int and casting to long2: " + castInt2 + " nanos");
    }

    private long measureTimeNs(Runnable fun) {
        long startT = System.nanoTime();
        fun.run();
        long endT = System.nanoTime();
        return (endT - startT);
    }

    private long measureTimeMs(Runnable fun) {
        long startT = System.currentTimeMillis();
        fun.run();
        long endT = System.currentTimeMillis();
        return (endT - startT);
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
            HyenaColumnHandle handle = new HyenaColumnHandle("", IntegerType.INTEGER, BlockType.I16Sparse, 0);
            return new HyenaRecordCursor(hyenaSession, Collections.singletonList(handle), TupleDomain.all());
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
