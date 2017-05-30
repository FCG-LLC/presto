package co.llective.presto.hyena.api;

import com.google.common.io.LittleEndianDataOutputStream;
import nanomsg.reqrep.ReqSocket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HyenaApi {
    private final ReqSocket s = new ReqSocket();

    public enum ScanComparison {
        Lt,
        LtEq,
        Eq,
        GtEq,
        Gt,
        NotEq
    }

    public static class ScanFilter {
        int column;
        ScanComparison op;
        long value;
    }

    public static class ScanRequest {
        long min_ts;
        long max_ts;
        List<ScanFilter> filters;
        List<Integer> projection;
    }

    public void connect() throws IOException {
        s.connect("ipc:///tmp/hyena.ipc");
    }

    public void scan(ScanRequest req) throws IOException {
        s.send(buildScanMessage(req));
    }

    byte[] buildScanMessage(ScanRequest req) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(baos);
        dos.writeInt(1);

        byte[] scanRequest = encodeScanRequest(req);
        baos.write(encodeByteArray(scanRequest));

        baos.close();

        return baos.toByteArray();
    }

    byte[] encodeScanRequest(ScanRequest req) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(baos);

        dos.writeLong(req.min_ts);
        dos.writeLong(req.max_ts);

        dos.writeLong(req.projection.size());
        for (Integer projected_column : req.projection) {
            dos.writeInt(projected_column);
        }

        dos.writeLong(req.filters.size());
        for (ScanFilter filter : req.filters) {
            dos.write(encodeScanFilter(filter));
        }

        return baos.toByteArray();
    }

    byte[] encodeScanFilter(ScanFilter filter) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(baos);

        dos.writeInt(filter.column);
        dos.writeInt(filter.op.ordinal());
        dos.writeLong(filter.value);

        return baos.toByteArray();
    }

    byte[] encodeByteArray(byte[] values) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(baos);
        dos.writeLong(values.length);
        dos.write(values);

//        byte[] bytes = baos.toByteArray();
//        System.out.println("Sending: "+bytes.length);
//        for (byte b:bytes) {
//            System.out.println(b);
//        }

        return baos.toByteArray();
    }

}
