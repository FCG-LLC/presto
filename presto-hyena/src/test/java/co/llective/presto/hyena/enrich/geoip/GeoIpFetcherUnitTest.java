package co.llective.presto.hyena.enrich.geoip;

import co.llective.presto.hyena.enrich.rest.RestClient;
import co.llective.presto.hyena.enrich.rest.RestClientException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;

import static co.llective.presto.hyena.enrich.geoip.GeoIpFetcher.TOUCAN_GEO_IP_ENDPOINT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class GeoIpFetcherUnitTest
{
    public static class Run
    {
        GeoIpFetcher fetcher;
        RestClient restClientMock;
        GeoIpCache cacheMock;
        ObjectMapper objectMapperMock;

        String resultJson = "asd";
        int resultJsonHashCode = resultJson.hashCode();

        LocalGeoIpEnrichment.LocalGeoIpEnrichments deserializedResults =
                new LocalGeoIpEnrichment.LocalGeoIpEnrichments(Collections.emptyList());

        @BeforeMethod
        public void setUp()
                throws RestClientException, IOException
        {
            restClientMock = mock(RestClient.class);
            when(restClientMock.getJson(TOUCAN_GEO_IP_ENDPOINT))
                    .thenReturn(resultJson);

            cacheMock = mock(GeoIpCache.class);
            doNothing().when(cacheMock).populateLocalGeoIp(any());

            objectMapperMock = mock(ObjectMapper.class);
            when(objectMapperMock.readValue(resultJson, LocalGeoIpEnrichment.LocalGeoIpEnrichments.class))
                    .thenReturn(deserializedResults);

            fetcher = new GeoIpFetcher(cacheMock, restClientMock, objectMapperMock);
        }

        @Test
        public void alwaysCallsRestClient()
                throws RestClientException
        {
            fetcher.run();
            fetcher.run();
            fetcher.run();
            verify(restClientMock, times(3))
                    .getJson(TOUCAN_GEO_IP_ENDPOINT);
        }

        @Test
        public void callsCacheUpdateWhenDataUpdated()
        {
            fetcher.setLastLocalGeoIpHashcode(resultJsonHashCode - 1);

            fetcher.run();

            verify(cacheMock).populateLocalGeoIp(deserializedResults.getEnrichedGeoIps());
        }

        @Test
        public void doesNotCallCacheWhenSameDataReturned()
        {
            fetcher.setLastLocalGeoIpHashcode(resultJsonHashCode);

            fetcher.run();

            verify(cacheMock, never()).populateLocalGeoIp(any());
        }

        @Test
        public void updatesHashCodeWhenDataUpdated()
        {
            int oldHashCode = fetcher.getLastLocalGeoIpHashcode();

            fetcher.run();

            assertNotEquals(fetcher.getLastLocalGeoIpHashcode(), oldHashCode);
        }

        @Test
        public void doesNotUpdateWhenRestFails()
                throws RestClientException
        {
            int oldHashCode = fetcher.getLastLocalGeoIpHashcode();

            when(restClientMock.getJson(any())).thenThrow(new RestClientException());

            fetcher.run();

            //doesn't update hashcode
            assertEquals(fetcher.getLastLocalGeoIpHashcode(), oldHashCode);
            //doesn't call cache
            verify(cacheMock, never()).populateLocalGeoIp(any());
        }

        @Test
        public void doesNotUpdateWhenProblemWithDeserialization()
                throws IOException
        {
            int oldHashCode = fetcher.getLastLocalGeoIpHashcode();

            when(objectMapperMock.readValue(resultJson, LocalGeoIpEnrichment.LocalGeoIpEnrichments.class))
                    .thenThrow(new IOException());

            fetcher.run();

            //doesn't update hashcode
            assertEquals(fetcher.getLastLocalGeoIpHashcode(), oldHashCode);
            //doesn't call cache
            verify(cacheMock, never()).populateLocalGeoIp(any());
        }
    }
}
