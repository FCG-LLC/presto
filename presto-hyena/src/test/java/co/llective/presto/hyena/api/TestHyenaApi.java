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
package co.llective.presto.hyena.api;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

public class TestHyenaApi
{
    HyenaApi api = new HyenaApi();
    HyenaApi.ScanFilter filter = new HyenaApi.ScanFilter();
    HyenaApi.ScanRequest req = new HyenaApi.ScanRequest();

    @BeforeTest
    public void setUp()
    {
        filter.op = HyenaApi.ScanComparison.GtEq;
        filter.column = 2;
        filter.value = 1000;
        filter.strValue = "";

        req.minTs = 100;
        req.maxTs = 200;
        req.filters = Arrays.asList(filter);
        req.projection = Arrays.asList(0, 1, 2, 3);
    }

    @Test
    public void testFilterEncoding() throws Exception
    {
        byte[] encoded = api.encodeScanFilter(filter);
        byte[] expected = new byte[] {
                2, 0, 0, 0, 3, 0, 0, 0, (byte) 232, 3, 0, 0, 0, 0, 0, 0
        };

        assertArrayEquals(expected, encoded);
    }

    @Test
    public void testRequestEncoding() throws Exception
    {
//        byte[] encoded = api.encodeScanRequest(req);
//        byte[] expected = new byte[] {
//                100, 0, 0, 0, 0, 0, 0, 0, (byte) 200, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, (byte) 232, 3, 0, 0, 0, 0, 0, 0
//        };
//
//        assertArrayEquals(expected, encoded);
    }

//    @Test
//    public void testApiMessage() throws Exception {
//        byte[] encoded = api.buildScanMessage(req);
//        byte[] expected = new byte[] {
//                1, 0, 0, 0, 60, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, (byte) 200, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, (byte) 232, 3, 0, 0, 0, 0, 0, 0
//        };
//
//        assertArrayEquals(expected, encoded);
//    }

    @Test
    public void testRefreshCatalogResponse() throws Exception
    {
        byte[] encoded = new byte[] {
                2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 116, 115, 2, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 115, 111, 117, 114, 99, 101, 1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, (byte) 200, 0, 0, 0, 0, 0, 0, 0, (byte) 231, 3, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 47, 102, 111, 111, 47, 98, 97, 114
        };

        ByteBuffer buf = ByteBuffer.wrap(encoded);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        // Just smoke test?
        HyenaApi.Catalog cat = api.decodeRefreshCatalog(buf);
        assertEquals(2, cat.columns.size());
        assertEquals(1, cat.availablePartitions.size());
    }

    @Test
    public void testSimpleRequest()
            throws Exception
    {
        HyenaApi.ScanRequest req = new HyenaApi.ScanRequest();
        req.minTs = 999;
        req.maxTs = 90000000;
        req.partitionId = 1775976771987511425L;

        HyenaApi.ScanFilter filter1 = new HyenaApi.ScanFilter();
        filter1.column = 2;
        filter1.op = HyenaApi.ScanComparison.LtEq;
        filter1.value = 50000000000000000L;
        filter1.strValue = "";
        req.filters = Arrays.asList(filter1);
        req.projection = Arrays.asList(0, 1, 2, 4, 23);

        api.connect("ipc:///tmp/hyena.ipc");

        HyenaApi.Catalog cat = api.refreshCatalog();
        System.out.println(cat.toString());

        HyenaApi.ScanResult res = api.scan(req, null);
        System.out.println(res);
    }
}
