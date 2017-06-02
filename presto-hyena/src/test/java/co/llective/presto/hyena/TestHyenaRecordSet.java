package co.llective.presto.hyena;


import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import org.testng.annotations.Test;

import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHyenaRecordSet {
    private static final HostAddress address = HostAddress.fromParts("localhost", 1234);

    @Test
    public void testSimpleCursor()
            throws Exception
    {
        HyenaTables hyenaTables = new HyenaTables(new HyenaConfig());
        HyenaSession hyenaSession = new NativeHyenaSession();
        HyenaMetadata metadata = new HyenaMetadata(hyenaTables);

        assertData(hyenaSession, metadata);
    }

    private static void assertData(HyenaSession hyenaSession, HyenaMetadata metadata)
    {
//        SchemaTableName tableName = metadata.listTables(hyenaSession, null).get(0);
//        List<HyenaColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, new HyenaTableHandle(tableName))
//                .values().stream().map(column -> (HyenaColumnHandle) column)
//                .collect(Collectors.toList());
//
//        HyenaRecordSet recordSet = new HyenaRecordSet(hyenaSession, new HyenaSplit(address, 1382217178546697565L, TupleDomain.all()), columnHandles);
//        RecordCursor cursor = recordSet.cursor();
//
//        for (int i = 0; i < columnHandles.size(); i++) {
//            assertEquals(cursor.getType(i), columnHandles.get(i).getColumnType());
//        }
//
//        for (int i = 0; i < 100; i++) {
//            assertTrue(cursor.advanceNextPosition());
//            System.out.println(String.format("ts: %d source: %d c1: %d", cursor.getLong(0), cursor.getLong(1), cursor.getLong(2)));
//        }
    }
}
