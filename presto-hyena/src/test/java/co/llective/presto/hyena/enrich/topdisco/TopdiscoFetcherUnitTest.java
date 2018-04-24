package co.llective.presto.hyena.enrich.topdisco;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TopdiscoFetcherUnitTest
{
    public static class Run
    {
        TopdiscoFetcher fetcher;
        ObjectMapper objectMapperMock;
        TopdiscoProvider topdiscoProviderMock;

        String responseJson = "responseJson";
        int responseHashCode = responseJson.hashCode();
        TopdiscoEnrichment deserializedResponse = new TopdiscoEnrichment(
                Collections.singletonList(new TopdiscoEnrichment.Ip("ip", "name", (short) 1)),
                Collections.singletonList(new TopdiscoEnrichment.Interface("80", 2, new HashSet<>(Collections.singletonList("ip2")))));

        @BeforeMethod
        public void setUp()
                throws SQLException, IOException
        {
            topdiscoProviderMock = mock(TopdiscoProvider.class);
            objectMapperMock = mock(ObjectMapper.class);
            when(objectMapperMock.readValue(responseJson, TopdiscoEnrichment.class))
                    .thenReturn(deserializedResponse);

            fetcher = spy(new TopdiscoFetcher(topdiscoProviderMock, objectMapperMock));

            doReturn(null).when(fetcher).fetchData();
        }

        @Test
        public void alwaysCallsDBForData()
                throws SQLException
        {
            fetcher.run();
            fetcher.run();
            fetcher.run();

            verify(fetcher, times(3)).fetchData();
        }

        @Test
        public void callsDeserializeJsonWhenHashcodeDiffers()
                throws SQLException, IOException
        {
            fetcher.setLastResponseHash(responseHashCode - 1);

            doReturn(responseJson).when(fetcher).fetchData();

            fetcher.run();

            verify(objectMapperMock).readValue(responseJson, TopdiscoEnrichment.class);
        }

        @Test
        public void updatesHashCodeWhenDeserializationDone()
                throws SQLException
        {
            fetcher.setLastResponseHash(responseHashCode - 1);

            doReturn(responseJson).when(fetcher).fetchData();

            fetcher.run();

            assertEquals(fetcher.getLastResponseHash(), responseHashCode);
        }

        @Test
        public void callsProviderUpdateWhenDeserializationDone()
                throws SQLException
        {
            fetcher.setLastResponseHash(responseHashCode - 1);

            doReturn(responseJson).when(fetcher).fetchData();

            fetcher.run();

            verify(topdiscoProviderMock).populateTopdiscoData(deserializedResponse);
        }

        @Test
        public void doesntUpdateHashCodeAndDeserializeWhenSQLFail()
                throws SQLException, IOException
        {
            doThrow(new SQLException()).when(fetcher).fetchData();

            int oldHashCode = fetcher.getLastResponseHash();

            fetcher.run();

            assertEquals(fetcher.getLastResponseHash(), oldHashCode);
            verify(objectMapperMock, never()).readValue(anyString(), same(TopdiscoEnrichment.class));
            verify(topdiscoProviderMock, never()).populateTopdiscoData(any());
        }

        @Test
        public void doesntUpdateHashCodeAndPopulatesDataWhenDeserializationFail()
                throws IOException, SQLException
        {
            doReturn(responseJson).when(fetcher).fetchData();
            doThrow(new IOException()).when(objectMapperMock).readValue(responseJson, TopdiscoEnrichment.class);

            int oldHashCode = fetcher.getLastResponseHash();

            fetcher.run();

            assertEquals(fetcher.getLastResponseHash(), oldHashCode);
            verify(topdiscoProviderMock, never()).populateTopdiscoData(any());
        }
    }
}
