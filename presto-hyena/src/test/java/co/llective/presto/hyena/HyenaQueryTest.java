package co.llective.presto.hyena;

import co.llective.hyena.api.*;
import com.facebook.presto.Session;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.LocalQueryRunner;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;


@PrepareForTest(HyenaClientModule.class)
@PowerMockIgnore({"org.mockito.*", "javax.management.*"})
public class HyenaQueryTest extends PowerMockTestCase
{
    NativeHyenaSession hyenaSession;
    List<Column> columns;
    List<PartitionInfo> partitions;

    LocalQueryRunner runner;
    Session prestoSession;

    private Catalog createCatalog() {
        columns = Arrays.asList(
                new Column(BlockType.I64Dense, 0, "ts"),
                new Column(BlockType.I32Dense, 1, "col1"),
                new Column(BlockType.I32Sparse, 2, "col2"),
                new Column(BlockType.StringBloomDense, 3, "strcol")
        );
        partitions = Arrays.asList(new PartitionInfo(0, 999999999, UUID.randomUUID(), "foobar"));
        return new Catalog(columns, partitions);
    }

    Catalog catalog = createCatalog();

    @BeforeTest
    public void setUpSession() throws Exception
    {
        hyenaSession = mock(NativeHyenaSession.class);
        when(hyenaSession.refreshCatalog()).thenReturn(catalog);
        when(hyenaSession.getAvailableColumns()).thenReturn(catalog.getColumns());
        when(hyenaSession.getAvailablePartitions()).thenReturn(catalog.getAvailablePartitions());
        when(hyenaSession.scan(any(ScanRequest.class))).thenReturn(new ScanResult(Collections.emptyMap()));

        PowerMockito.mockStatic(HyenaClientModule.class);
        when(HyenaClientModule.createHyenaSession(any(HyenaConfig.class))).thenReturn(hyenaSession);

         prestoSession = Session.builder(new SessionPropertyManager())
                 .setQueryId(new QueryId("test"))
                 .setIdentity(new Identity("foo", Optional.empty()))
                 .setCatalog(HyenaMetadata.PRESTO_HYENA_SCHEMA)
                 .build();

        runner = new LocalQueryRunner(prestoSession);
        runner.createCatalog(HyenaMetadata.PRESTO_HYENA_SCHEMA, new HyenaConnectorFactory(), new HashMap<>());
    }

//    @Test
//    public void testAndQuery()
//    {
//        this.runner.execute("SELECT * from hyena.cs WHERE col1 IN (1,2,3) AND col2 > 5");
//    }

//    @Test
//    public void testOrQuery()
//    {
//        this.runner.execute("SELECT * from hyena.cs WHERE col1 IN (1,2,3) OR col2 > 5");
//    }

    @Test
    public void testStringOrQuery()
    {
        this.runner.execute("SELECT * from hyena.cs WHERE col1 IN (1,2,3) AND (strcol = '%foobar%' OR strcol = '*m*o*r*e')");
    }

//    @Test
//    public void testOrWithInequalityQuery()
//    {
//        this.runner.execute("SELECT * from hyena.cs WHERE col1 IN (1,2,3) AND (col2 = 1OR col2 > 5)");
//    }

}
