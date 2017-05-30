package co.llective.presto.hyena.api;

import jdk.nashorn.internal.ir.annotations.Ignore;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

public class TestHyenaApi {

    HyenaApi api = new HyenaApi();
    HyenaApi.ScanFilter filter = new HyenaApi.ScanFilter();
    HyenaApi.ScanRequest req = new HyenaApi.ScanRequest();

    @BeforeTest
    public void setUp() {
        filter.op = HyenaApi.ScanComparison.GtEq;
        filter.value = 1000;

        req.min_ts = 100;
        req.max_ts = 200;
        req.filters = Arrays.asList(filter);
        req.projection = Arrays.asList(0,1,2,3);
    }

    @Test
    public void testFilterEncoding() throws Exception {
        byte[] encoded = api.encodeScanFilter(filter);
        byte[] expected = new byte[] {
                3, 0, 0, 0, (byte) 232, 3, 0, 0, 0, 0, 0, 0
        };

        assertArrayEquals(expected, encoded);
    }

    @Test
    public void testRequestEncoding() throws Exception {
        byte[] encoded = api.encodeScanRequest(req);
        byte[] expected = new byte[] {
                100, 0, 0, 0, 0, 0, 0, 0, (byte) 200, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, (byte) 232, 3, 0, 0, 0, 0, 0, 0
        };

        assertArrayEquals(expected, encoded);
    }

    public void testApiMessage() throws Exception {
        byte[] encoded = api.buildScanMessage(req);
        byte[] expected = new byte[] {
                1, 0, 0, 0, 60, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, (byte) 200, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, (byte) 232, 3, 0, 0, 0, 0, 0, 0
        };

        assertArrayEquals(expected, encoded);
    }

    @Test
    public void testSimpleRequest()
            throws Exception
    {
        HyenaApi.ScanRequest req = new HyenaApi.ScanRequest();
        req.min_ts = 999;
        req.max_ts = 90000000;
        HyenaApi.ScanFilter filter1 = new HyenaApi.ScanFilter();
        filter1.op = HyenaApi.ScanComparison.GtEq;
        filter1.value = 500000;
        req.filters = Arrays.asList(filter1);
        req.projection = Arrays.asList(0,1,2,4);

        api.connect();
        api.scan(req);
    }

}
