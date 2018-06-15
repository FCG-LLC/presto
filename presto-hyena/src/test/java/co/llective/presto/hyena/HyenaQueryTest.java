package co.llective.presto.hyena;

import co.llective.hyena.api.BlockType;
import co.llective.hyena.api.Catalog;
import co.llective.hyena.api.Column;
import co.llective.hyena.api.HyenaApi;
import co.llective.hyena.api.PartitionInfo;
import com.facebook.presto.Session;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.LocalQueryRunner;
import com.sun.jna.Native;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HyenaQueryTest
{
    NativeHyenaSession hyenaSession = mock(NativeHyenaSession.class);
    List<Column> columns;
    List<PartitionInfo> partitions;

    LocalQueryRunner runner;
    Session prestoSession;

    private Catalog createCatalog() {
        columns = Arrays.asList(
                new Column(BlockType.U64Dense, 0, "ts"),
                new Column(BlockType.U32Dense, 1, "col1"),
                new Column(BlockType.U32Sparse, 2, "col2")
        );
        partitions = Arrays.asList(new PartitionInfo(0, 999999999, UUID.randomUUID(), "foobar"));
        return new Catalog(columns, partitions);
    }

    @BeforeTest
    public void setUpSession() throws Exception
    {
//        HyenaApi pseudoApi = mock(HyenaApi.class);
//        PowerMockito.whenNew(HyenaApi.class).withAnyArguments().thenReturn(pseudoApi);


        PowerMockito.whenNew(NativeHyenaSession.class).withAnyArguments().thenReturn(hyenaSession);
        when(hyenaSession.refreshCatalog()).thenReturn(createCatalog());


         prestoSession = Session.builder(new SessionPropertyManager())
                 .setQueryId(new QueryId("test"))
                 .setIdentity(new Identity("foo", Optional.empty()))
//                 .setSchema(HyenaMetadata.PRESTO_HYENA_SCHEMA)
                 .setCatalog(HyenaMetadata.PRESTO_HYENA_SCHEMA)
                 .build();

        runner = new LocalQueryRunner(prestoSession);
        runner.createCatalog(HyenaMetadata.PRESTO_HYENA_SCHEMA, new HyenaConnectorFactory(), new HashMap<>());
    }

    @Test
    public void testQuery()
    {
        this.runner.execute("SELECT * from hyena.hyena");
    }
}
