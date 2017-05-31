package co.llective.presto.hyena.api;

import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.io.LittleEndianDataOutputStream;
import nanomsg.Nanomsg;
import nanomsg.reqrep.ReqSocket;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import javax.xml.crypto.Data;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class HyenaApi {
    private final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    private final ReqSocket s = new ReqSocket();

    public enum ApiRequest {
        Insert,
        Scan,
        RefreshCatalog
    }

    public enum ScanComparison {
        Lt,
        LtEq,
        Eq,
        GtEq,
        Gt,
        NotEq
    }

    public static class ScanFilter {
        public int column;
        public ScanComparison op;
        public long value;

        public ScanFilter() {}

        public ScanFilter(int column, ScanComparison op, long value) {
            this.column = column;
            this.op = op;
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("%d %s %d", column, op.name(), value);
        }
    }

    public static class ScanRequest {
        public long min_ts;
        public long max_ts;
        public long partitionId;
        public List<ScanFilter> filters;
        public List<Integer> projection;
    }

    public static class DenseBlock<T> {
        public List<T> data = new ArrayList<>();

        public T get(int offset) {
            return data.get(offset);
        }
    }

    public static class SparseBlock<T> {
        public List<Pair<Integer, T>> data = new ArrayList<>();

        private int currentPosition = 0;

        public T getMaybe(int offset) {
            while (currentPosition < data.size() && data.get(currentPosition).getLeft() < offset) {
                currentPosition++;
            }

            if (currentPosition < data.size() && data.get(currentPosition).getLeft() == offset) {
                return data.get(currentPosition).getRight();
            }

            return null;
        }
    }

    public static class BlockHolder {
        public BlockType type;
        public DenseBlock<Long> int64DenseBlock;
        public SparseBlock<Long> int64SparseBlock;
        public SparseBlock<Integer> int32SparseBlock;

        @Override
        public String toString() {
            int count = 0;
            switch(type) {
                case Int32Sparse:
                    count = int32SparseBlock.data.size();
                    break;
                case Int64Sparse:
                    count = int64SparseBlock.data.size();
                    break;
                case Int64Dense:
                    count = int64DenseBlock.data.size();
                    break;
            }

            return String.format("%s with %d elements", type.name(), count);
        }

        public static BlockHolder decode(DataInput in) throws IOException {
            BlockHolder holder = new BlockHolder();
            holder.type = BlockType.values()[in.readInt()];

            switch(holder.type) {
                case Int32Sparse:
                    holder.int32SparseBlock = new SparseBlock<>();
                    break;
                case Int64Sparse:
                    holder.int64SparseBlock = new SparseBlock<>();
                    break;
                case Int64Dense:
                    holder.int64DenseBlock = new DenseBlock<>();
                    break;
            }

            long records = in.readLong();
            for (int i = 0; i < records; i++) {
                switch(holder.type) {
                    case Int32Sparse:
                        holder.int32SparseBlock.data.add(Pair.of(in.readInt(), in.readInt()));
                        break;
                    case Int64Sparse:
                        holder.int64SparseBlock.data.add(Pair.of(in.readInt(), in.readLong()));
                        break;
                    case Int64Dense:
                        holder.int64DenseBlock.data.add(in.readLong());
                        break;
                }
            }

            return holder;
        }

    }

    public static class ScanResult {
        public int row_count;
        public int col_count;
        public List<Pair<Integer, BlockType>> col_types;
        public List<BlockHolder> blocks;

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();

            sb.append(String.format("Result having %d rows with %d columns. ", row_count, col_count));
            sb.append(StringUtils.join(blocks, ", "));
            return sb.toString();
        }

        public ScanResult(int row_count, int col_count, List<Pair<Integer, BlockType>> col_types, List<BlockHolder> blocks) {
            this.row_count = row_count;
            this.col_count = col_count;
            this.col_types = col_types;
            this.blocks = blocks;
        }

        private static List<Pair<Integer, BlockType>> decodeColumnTypes(DataInput in) throws IOException {
            long colCount = in.readLong();
            List<Pair<Integer, BlockType>> colTypes = new ArrayList<>();

            for (int i = 0; i < colCount; i++) {
                colTypes.add(Pair.of(in.readInt(), BlockType.values()[in.readInt()]));
            }

            return colTypes;
        }

        private static List<BlockHolder> decodeBlocks(DataInput in) throws IOException {
            long count = in.readLong();
            List<BlockHolder> blocks = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                blocks.add(BlockHolder.decode(in));
            }
            return blocks;
        }

        public static ScanResult decode(DataInput in) throws IOException {
            int row_count = in.readInt();
            int col_count = in.readInt();

            return new ScanResult(
                    row_count,
                    col_count,
                    ScanResult.decodeColumnTypes(in),
                    ScanResult.decodeBlocks(in)
            );
        }


    }

    public enum BlockType {
        Int64Dense,
        Int64Sparse,
        Int32Sparse
    }

    public static class Column {
        public BlockType data_type;
        public String name;

        public Column(BlockType data_type, String name) {
            this.data_type = data_type;
            this.name = name;
        }

        @Override
        public String toString() {
            return String.format("%s %s", name, data_type.name());
        }
    }

    public static class PartitionInfo {
        public long min_ts;
        public long max_ts;
        public long id;
        public String location;

        public PartitionInfo(long min_ts, long max_ts, long id, String location) {
            this.min_ts = min_ts;
            this.max_ts = max_ts;
            this.id = id;
            this.location = location;
        }

        @Override
        public String toString() {
            return String.format("%d [%d-%d]", this.id, this.min_ts, this.max_ts);
        }
    }

    public static class Catalog {
        public List<Column> columns;
        public List<PartitionInfo> availablePartitions;

        public Catalog(List<Column> columns, List<PartitionInfo> availablePartitions) {
            this.columns = columns;
            this.availablePartitions = availablePartitions;
        }

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append("Columns: ");
            sb.append(StringUtils.join(columns, ", "));
            sb.append("Partitions: ");
            sb.append(StringUtils.join(availablePartitions, ", "));
            return sb.toString();
        }
    }

    private boolean connected = false;

    private void ensureConnected() {
        try {
             if (!connected) {
                 connect();
             }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public void connect() throws IOException {
        s.setRecvTimeout(60000);
        s.setSendTimeout(60000);
        s.connect("ipc:///tmp/hyena.ipc");
        this.connected = true;
    }

    public ScanResult scan(ScanRequest req) throws IOException {
        ensureConnected();

        s.send(buildScanMessage(req));
        try {
            byte[] response = s.recvBytes();
            LittleEndianDataInputStream in = new LittleEndianDataInputStream(new ByteArrayInputStream(response));
            return ScanResult.decode(in);
        } catch (Throwable t) {
            System.out.println("Nanomsg error: "+Nanomsg.getError());
            throw new IOException("Nanomsg error: "+Nanomsg.getError(), t);
        }

    }

    public Catalog refreshCatalog() throws IOException {
        ensureConnected();

        s.send(buildRefreshCatalogMessage());
        // FIXME: we should be using bytebuffer
        byte[] response = s.recvBytes();
        LittleEndianDataInputStream in = new LittleEndianDataInputStream(new ByteArrayInputStream(response));
        return decodeRefreshCatalog(in);
    }

    byte[] buildRefreshCatalogMessage() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(baos);
        dos.writeInt(ApiRequest.RefreshCatalog.ordinal());
        dos.writeLong(0);
        baos.close();

        return baos.toByteArray();
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
        dos.writeLong(req.partitionId);

        dos.writeLong(req.projection.size());
        for (Integer projected_column : req.projection) {
            dos.writeInt(projected_column);
        }

        if (req.filters == null) {
            dos.writeLong(0);
        } else {
            dos.writeLong(req.filters.size());
            for (ScanFilter filter : req.filters) {
                dos.write(encodeScanFilter(filter));
            }
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

    Catalog decodeRefreshCatalog(DataInput in) throws IOException {
        long columnCount = in.readLong();
        List<Column> columns = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            columns.add(decodeColumn(in));
        }

        long partitionCount = in.readLong();
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            partitions.add(decodePartitionInfo(in));
        }

        return new Catalog(columns, partitions);
    }

    PartitionInfo decodePartitionInfo(DataInput in) throws IOException {
        return new PartitionInfo(in.readLong(), in.readLong(), in.readLong(), decodeStringArray(in));
    }

    Column decodeColumn(DataInput in) throws IOException {
        return new Column(
                BlockType.values()[in.readInt()],
                decodeStringArray(in)
        );
    }

    String decodeStringArray(DataInput is) throws IOException {
        int len = (int) is.readLong();
        byte[] buf = new byte[len];
        is.readFully(buf, 0, len);
        return new String(buf, UTF8_CHARSET);
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
