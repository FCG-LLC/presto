package co.llective.presto.hyena.api;

import co.llective.presto.hyena.NativeHyenaSession;
import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.io.LittleEndianDataOutputStream;
import io.airlift.log.Logger;
import nanomsg.Nanomsg;
import nanomsg.reqrep.ReqSocket;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import javax.xml.crypto.Data;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HyenaApi {
    private static final Logger log = Logger.get(HyenaApi.class);

    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

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
        public String strValue;

        public ScanFilter() {}

        public ScanFilter(int column, ScanComparison op, long value, String strValue) {
            this.column = column;
            this.op = op;
            this.value = value;
            this.strValue = strValue;
        }

        @Override
        public String toString() {
            return String.format("%d %s %d/%s", column, op.name(), value, strValue);
        }
    }

    public static class ScanFilterBuilder {
        private ScanFilter filter = new ScanFilter();
        private boolean columnSet = false;
        private boolean opSet = false;
        private boolean filterValSet = false;

        public ScanFilterBuilder() {
            this.filter.strValue = "";
            this.filter.value = 0;
        }

        public ScanFilterBuilder withColumn(int column) {
            columnSet = true;
            filter.column = column;
            return this;
        }
        public ScanFilterBuilder withOp(ScanComparison op) {
            opSet = true;
            filter.op = op;
            return this;
        }
        public ScanFilterBuilder withLongValue(long val) {
            filterValSet = true;
            filter.value = val;
            return this;
        }
        public ScanFilterBuilder withStringValue(String val) {
            filterValSet = true;
            filter.strValue = val;
            return this;
        }
        public ScanFilter build() {
            assert(filterValSet);
            assert(columnSet);
            assert(opSet);

            return filter;
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

        public DenseBlock() {}
        public DenseBlock(int size) {
            this.data = new ArrayList<>(size);
        }

        public T get(int offset) {
            return data.get(offset);
        }
    }

    public static class StringBlock {
        public List<Integer> offsetData = new ArrayList<>();
        public List<Long> valueStartPositions = new ArrayList<>();
        public byte[] bytes;

        private int currentPosition = 0;

        public StringBlock() {}
        public StringBlock(int size) {
            offsetData = new ArrayList<>(size);
            valueStartPositions = new ArrayList<>(size);
        }

        public String getMaybe(int offset) {
            while (currentPosition < offsetData.size() && offsetData.get(currentPosition) < offset) {
                currentPosition++;
            }

            if (currentPosition < offsetData.size() && offsetData.get(currentPosition) == offset) {
                int startPosition = valueStartPositions.get(currentPosition).intValue();
                int endPosition;

                if (currentPosition == offsetData.size()-1) {
                    endPosition = bytes.length;
                } else {
                    endPosition = valueStartPositions.get(currentPosition+1).intValue();
                }

                byte[] strBytes = Arrays.copyOfRange(bytes, startPosition, endPosition);
                return new String(strBytes, UTF8_CHARSET);
            }

            return null;
        }
    }

    public static class SparseBlock<T> {
        public List<Integer> offsetData = new ArrayList<>();
        public List<T> valueData = new ArrayList<>();

        private int currentPosition = 0;

        public SparseBlock() {}
        public SparseBlock(int size) {
            offsetData = new ArrayList<>(size);
            valueData = new ArrayList<>(size);
        }

        public T getMaybe(int offset) {
            while (currentPosition < offsetData.size() && offsetData.get(currentPosition) < offset) {
                currentPosition++;
            }

            if (currentPosition < offsetData.size() && offsetData.get(currentPosition) == offset) {
                return valueData.get(currentPosition);
            }

            return null;
        }
    }

    public static class BlockHolder {
        public BlockType type;
        public DenseBlock<Long> int64DenseBlock;
        public SparseBlock<Long> int64SparseBlock;
        public SparseBlock<Integer> int32SparseBlock;
        public SparseBlock<Short> int16SparseBlock;
        public SparseBlock<Byte> int8SparseBlock;
        public StringBlock stringBlock;

        @Override
        public String toString() {
            int count = 0;
            switch(type) {
                case Int8Sparse:
                    count = int8SparseBlock.offsetData.size();
                    break;
                case Int16Sparse:
                    count = int16SparseBlock.offsetData.size();
                    break;
                case Int32Sparse:
                    count = int32SparseBlock.offsetData.size();
                    break;
                case Int64Sparse:
                    count = int64SparseBlock.offsetData.size();
                    break;
                case Int64Dense:
                    count = int64DenseBlock.data.size();
                    break;
                case String:
                    count = stringBlock.offsetData.size();
                    break;
            }

            return String.format("%s with %d elements", type.name(), count);
        }

        public static BlockHolder decode(ByteBuffer buf) throws IOException {
            BlockHolder holder = new BlockHolder();
            holder.type = BlockType.values()[buf.getInt()];
            int recordsCount = (int) buf.getLong();

            switch(holder.type) {
                case Int8Sparse:
                    holder.int8SparseBlock = new SparseBlock<>(recordsCount);
                    break;
                case Int16Sparse:
                    holder.int16SparseBlock = new SparseBlock<>(recordsCount);
                    break;
                case Int32Sparse:
                    holder.int32SparseBlock = new SparseBlock<>(recordsCount);
                    break;
                case Int64Sparse:
                    holder.int64SparseBlock = new SparseBlock<>(recordsCount);
                    break;
                case Int64Dense:
                    holder.int64DenseBlock = new DenseBlock<>(recordsCount);
                    break;
                case String:
                    holder.stringBlock = new StringBlock(recordsCount);
                    break;
            }

            for (int i = 0; i < recordsCount; i++) {
                switch(holder.type) {
                    case Int8Sparse:
                        holder.int8SparseBlock.offsetData.add(buf.getInt());
                        holder.int8SparseBlock.valueData.add(buf.get());
                        break;
                    case Int16Sparse:
                        holder.int16SparseBlock.offsetData.add(buf.getInt());
                        holder.int16SparseBlock.valueData.add(buf.getShort());
                        break;
                    case Int32Sparse:
                        holder.int32SparseBlock.offsetData.add(buf.getInt());
                        holder.int32SparseBlock.valueData.add(buf.getInt());
                        break;
                    case Int64Sparse:
                        holder.int64SparseBlock.offsetData.add(buf.getInt());
                        holder.int64SparseBlock.valueData.add(buf.getLong());
                        break;
                    case Int64Dense:
                        holder.int64DenseBlock.data.add(buf.getLong());
                        break;
                    case String:
                        holder.stringBlock.offsetData.add(buf.getInt());
                        holder.stringBlock.valueStartPositions.add(buf.getLong());
                }
            }

            if (holder.type == BlockType.String) {
                int len = (int) buf.getLong();
                holder.stringBlock.bytes = new byte[len];
                buf.get(holder.stringBlock.bytes, 0, len);
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

        private static List<Pair<Integer, BlockType>> decodeColumnTypes(ByteBuffer buf) throws IOException {
            long colCount = buf.getLong();
            List<Pair<Integer, BlockType>> colTypes = new ArrayList<>();

            for (int i = 0; i < colCount; i++) {
                colTypes.add(Pair.of(buf.getInt(), BlockType.values()[buf.getInt()]));
            }

            return colTypes;
        }

        private static List<BlockHolder> decodeBlocks(ByteBuffer buf) throws IOException {
            long count = buf.getLong();
            List<BlockHolder> blocks = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                blocks.add(BlockHolder.decode(buf));
            }
            return blocks;
        }

        public static ScanResult decode(ByteBuffer buf) throws IOException {
            int row_count = buf.getInt();
            int col_count = buf.getInt();

            log.info("Received ResultSet with %d rows", row_count);

            return new ScanResult(
                    row_count,
                    col_count,
                    ScanResult.decodeColumnTypes(buf),
                    ScanResult.decodeBlocks(buf)
            );
        }


    }

    public enum BlockType {
        Int64Dense,
        Int64Sparse,
        Int32Sparse,
        Int16Sparse,
        Int8Sparse,
        String
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

    private synchronized void ensureConnected() {
        try {
             if (!connected) {
                 connect();
             }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public void connect() throws IOException {
        String handle =  "ipc:///tmp/hyena.ipc";
        log.info("Opening new connection to: "+handle);
        s.setRecvTimeout(60000);
        s.setSendTimeout(60000);
        s.connect(handle);
        log.info("Connection successfully opened");
        this.connected = true;
    }

    public void close() {
        s.close();
    }

    public static class HyenaOpMetadata {
        public int bytes;
    }

    public ScanResult scan(ScanRequest req, HyenaOpMetadata metaOrNull) throws IOException {
        ensureConnected();

        s.send(buildScanMessage(req));
        log.info("Sent scan request to partition " + req.partitionId);
        try {
            log.info("Waiting for scan response from partition " + req.partitionId);
            ByteBuffer buf = s.recv();
            log.info("Received scan response from partition " + req.partitionId);
            buf.order(ByteOrder.LITTLE_ENDIAN);

            ScanResult result = ScanResult.decode(buf);

            if (metaOrNull != null) {
                metaOrNull.bytes = buf.position();
            }

            return result;
        } catch (Throwable t) {
            log.error("Nanomsg error: "+Nanomsg.getError());
            throw new IOException("Nanomsg error: "+Nanomsg.getError(), t);
        }

    }

    public Catalog refreshCatalog() throws IOException {
        ensureConnected();

        s.send(buildRefreshCatalogMessage());

        ByteBuffer buf = s.recv();
        buf.order(ByteOrder.LITTLE_ENDIAN);
        return decodeRefreshCatalog(buf);
    }

    byte[] buildRefreshCatalogMessage() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(baos);
        dos.writeInt(ApiRequest.RefreshCatalog.ordinal());
        dos.writeLong(0L); // 0 bytes for payload
        baos.close();

        return baos.toByteArray();
    }

    byte[] buildScanMessage(ScanRequest req) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(baos);
        dos.writeInt(ApiRequest.Scan.ordinal());

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
        dos.write(encodeStringArray(filter.strValue));

        return baos.toByteArray();
    }

    Catalog decodeRefreshCatalog(ByteBuffer buf) throws IOException {
        long columnCount = buf.getLong();
        List<Column> columns = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            columns.add(decodeColumn(buf));
        }

        long partitionCount = buf.getLong();
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            partitions.add(decodePartitionInfo(buf));
        }

        return new Catalog(columns, partitions);
    }

    PartitionInfo decodePartitionInfo(ByteBuffer buf) throws IOException {
        return new PartitionInfo(buf.getLong(), buf.getLong(), buf.getLong(), decodeStringArray(buf));
    }

    Column decodeColumn(ByteBuffer buf) throws IOException {
        return new Column(
                BlockType.values()[buf.getInt()],
                decodeStringArray(buf)
        );
    }

    byte[] encodeStringArray(String str) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(baos);

        byte[] strBytes = str.getBytes(UTF8_CHARSET);
        dos.writeLong(strBytes.length);
        dos.write(strBytes);

        return baos.toByteArray();
    }


    String decodeStringArray(ByteBuffer buf) throws IOException {
        int len = (int) buf.getLong();

        byte[] bytes = new byte[len];
        buf.get(bytes, 0, len);
        return new String(bytes, UTF8_CHARSET);
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
