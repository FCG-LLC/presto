package co.llective.presto.hyena.enrich.username;

import co.llective.presto.hyena.enrich.rest.RestClient;
import co.llective.presto.hyena.enrich.rest.RestClientException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class UserCacheFetcherUnitTest
{
    public static class Run
    {
        UserCacheFetcher fetcher;
        RestClient restClientMock;
        UserNameCache cacheMock;
        ObjectMapper objectMapperMock;

        String resultJson = "asd";
        int resultJsonHashCode = resultJson.hashCode();

        @BeforeMethod
        public void setUp()
                throws RestClientException, IOException
        {
            restClientMock = mock(RestClient.class);
            when(restClientMock.getJson(any())).thenReturn(resultJson);

            cacheMock = mock(UserNameCache.class);
            doNothing().when(cacheMock).populateEnrichedUsers(any());

            objectMapperMock = mock(ObjectMapper.class);
            when(objectMapperMock.readValue(resultJson, EnrichedUser.EnrichedUsers.class)).thenReturn(
                    new EnrichedUser.EnrichedUsers(Collections.emptyList()));

            fetcher = new UserCacheFetcher(cacheMock, restClientMock, objectMapperMock);
        }

        @Test
        public void alwaysCallsRestClient()
                throws RestClientException
        {
            fetcher.run();
            fetcher.run();
            fetcher.run();
            verify(restClientMock, times(3)).getJson(any());
        }

        @Test
        public void callsCacheUpdateWhenDataUpdated()
        {
            fetcher.setLastUserCacheHashcode(resultJsonHashCode + 1);

            fetcher.run();

            verify(cacheMock).populateEnrichedUsers(any());
        }

        @Test
        public void doesNotCallCacheWhenSameDataReturned()
        {
            fetcher.setLastUserCacheHashcode(resultJsonHashCode);

            fetcher.run();

            verify(cacheMock, never()).populateEnrichedUsers(any());
        }

        @Test
        public void updatesHashCodeWhenDataUpdated()
        {
            int oldHashCode = fetcher.getLastUserCacheHashcode();

            fetcher.run();

            assertNotEquals(fetcher.getLastUserCacheHashcode(), oldHashCode);
        }

        @Test
        public void doesNotUpdateWhenRestFails()
                throws RestClientException
        {
            int oldHashCode = fetcher.getLastUserCacheHashcode();

            when(restClientMock.getJson(any())).thenThrow(new RestClientException());

            fetcher.run();

            //doesn't update hashcode
            assertEquals(fetcher.getLastUserCacheHashcode(), oldHashCode);
            //doesn't call cache
            verify(cacheMock, never()).populateEnrichedUsers(any());
        }

        @Test
        public void doesNotUpdateWhenProblemWithDeserialization()
                throws IOException
        {
            int oldHashCode = fetcher.getLastUserCacheHashcode();

            when(objectMapperMock.readValue(resultJson, EnrichedUser.EnrichedUsers.class)).thenThrow(
                    new IOException());

            fetcher.run();

            //doesn't update hashcode
            assertEquals(fetcher.getLastUserCacheHashcode(), oldHashCode);
            //doesn't call cache
            verify(cacheMock, never()).populateEnrichedUsers(any());
        }
    }
}
