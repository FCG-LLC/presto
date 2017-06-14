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

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHyenaRecordSet
{
    private static final HostAddress address = HostAddress.fromParts("localhost", 1234);

    @Test
    public void testSimpleCursor()
            throws Exception
    {
        HyenaConfig config = new HyenaConfig();
        config.setHyenaHost("ipc:///tmp/hyena.ipc");

        HyenaTables hyenaTables = new HyenaTables(config);
        HyenaSession hyenaSession = new NativeHyenaSession(config);
        HyenaMetadata metadata = new HyenaMetadata(hyenaTables);

        assertData(hyenaSession, metadata);
    }

    private static void assertData(HyenaSession hyenaSession, HyenaMetadata metadata)
    {
        SchemaTableName tableName = metadata.listTables(null, null).get(0);
        List<HyenaColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, new HyenaTableHandle(tableName))
                .values().stream().map(column -> (HyenaColumnHandle) column)
                .collect(Collectors.toList());

        HyenaRecordSet recordSet = new HyenaRecordSet(hyenaSession, new HyenaSplit(address, 1775976771987511425L, TupleDomain.all()), columnHandles);
        RecordCursor cursor = recordSet.cursor();

        for (int i = 0; i < columnHandles.size(); i++) {
            assertEquals(cursor.getType(i), columnHandles.get(i).getColumnType());
        }

        for (int i = 0; i < 100; i++) {
            assertTrue(cursor.advanceNextPosition());
            System.out.println(String.format("ts: %d source: %d c1: %d c24: %s", cursor.getLong(0), cursor.getLong(1), cursor.getLong(2), new String(cursor.getSlice(24).getBytes())));
        }
    }
}
