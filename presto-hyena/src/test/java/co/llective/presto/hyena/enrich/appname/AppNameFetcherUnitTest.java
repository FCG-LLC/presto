package co.llective.presto.hyena.enrich.appname;

import co.llective.presto.hyena.enrich.rest.RestClient;
import co.llective.presto.hyena.enrich.rest.RestClientException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.LinkedHashMap;

import static co.llective.presto.hyena.enrich.appname.ApplicationNameFetcher.TOUCAN_APP_GLOBAL_ENDPOINT;
import static co.llective.presto.hyena.enrich.appname.ApplicationNameFetcher.TOUCAN_APP_USER_ENDPOINT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AppNameFetcherUnitTest
{
    public static class Run
    {
        ApplicationNameFetcher fetcher;
        RestClient restClientMock;
        ApplicationNameCache cacheMock;
        ObjectMapper objectMapperMock;

        String userToucanJson = "userJson";
        String globalToucanJson = "globalJson";

        EnrichedAppNames deserializedUserJson = createDeserializedUserJson();
        EnrichedAppNames deserializedGlobalJson = createDeserializedGlobalJson();

        private EnrichedAppNames createDeserializedUserJson()
        {
            LinkedHashMap<String, String> names = new LinkedHashMap<>();
            names.put("11.2.0.0/16", "n1");
            LinkedHashMap<Integer, String> ports = new LinkedHashMap<>();
            ports.put(8034, "p1");
            return new EnrichedAppNames(names, ports);
        }

        private EnrichedAppNames createDeserializedGlobalJson()
        {
            LinkedHashMap<String, String> names = new LinkedHashMap<>();
            names.put("10.2.0.0/16", "n2");
            LinkedHashMap<Integer, String> ports = new LinkedHashMap<>();
            ports.put(8035, "p2");
            return new EnrichedAppNames(names, ports);
        }

        private EnrichedAppNames createMergedAppNames()
        {
            LinkedHashMap<String, String> names = new LinkedHashMap<>();
            names.put("11.2.0.0/16", "n1");
            names.put("10.2.0.0/16", "n2");
            LinkedHashMap<Integer, String> ports = new LinkedHashMap<>();
            ports.put(8034, "p1");
            ports.put(8035, "p2");
            return new EnrichedAppNames(names, ports);
        }

        @BeforeMethod
        public void setUp()
                throws IOException, RestClientException
        {
            restClientMock = mock(RestClient.class);
            when(restClientMock.getJson(TOUCAN_APP_USER_ENDPOINT))
                    .thenReturn(userToucanJson);
            when(restClientMock.getJson(TOUCAN_APP_GLOBAL_ENDPOINT))
                    .thenReturn(globalToucanJson);

            cacheMock = mock(ApplicationNameCache.class);
            doNothing().when(cacheMock).populateEnrichedAppNames(any());

            objectMapperMock = mock(ObjectMapper.class);
            when(objectMapperMock.readValue(userToucanJson, EnrichedAppNames.class))
                    .thenReturn(deserializedUserJson);
            when(objectMapperMock.readValue(globalToucanJson, EnrichedAppNames.class))
                    .thenReturn(deserializedGlobalJson);

            fetcher = new ApplicationNameFetcher(cacheMock, restClientMock, objectMapperMock);
        }

        @Test
        public void alwaysCallsRestClientTwice()
                throws RestClientException
        {
            fetcher.run();
            fetcher.run();
            fetcher.run();

            verify(restClientMock, times(3))
                    .getJson(TOUCAN_APP_USER_ENDPOINT);
            verify(restClientMock, times(3))
                    .getJson(TOUCAN_APP_GLOBAL_ENDPOINT);
        }

        @Test
        public void callsCacheWithGlobalWhenUserRestFails()
                throws RestClientException
        {
            when(restClientMock.getJson(TOUCAN_APP_USER_ENDPOINT))
                    .thenThrow(new RestClientException());

            fetcher.run();

            verify(cacheMock).populateEnrichedAppNames(deserializedGlobalJson);
        }

        @Test
        public void callsCacheWithUserWhenGlobalRestFails()
                throws RestClientException
        {
            when(restClientMock.getJson(TOUCAN_APP_GLOBAL_ENDPOINT))
                    .thenThrow(new RestClientException());

            fetcher.run();

            verify(cacheMock).populateEnrichedAppNames(deserializedUserJson);
        }

        @Test
        public void callsCacheWithGlobalWhenUserDeserializeFails()
                throws IOException
        {
            when(objectMapperMock.readValue(userToucanJson, EnrichedAppNames.class))
                    .thenThrow(new IOException());

            fetcher.run();

            verify(cacheMock).populateEnrichedAppNames(deserializedGlobalJson);
        }

        @Test
        public void callsCacheWithUserWhenGlobalDeserializeFails()
                throws IOException
        {
            when(objectMapperMock.readValue(globalToucanJson, EnrichedAppNames.class))
                    .thenThrow(new IOException());

            fetcher.run();

            verify(cacheMock).populateEnrichedAppNames(deserializedUserJson);
        }

        @Test
        public void callsCacheWithEmptyValuesWhenBothFail()
                throws IOException, RestClientException
        {
            when(objectMapperMock.readValue(globalToucanJson, EnrichedAppNames.class))
                    .thenThrow(new IOException());
            when(restClientMock.getJson(TOUCAN_APP_USER_ENDPOINT))
                    .thenThrow(new RestClientException());

            fetcher.run();

            verify(cacheMock).populateEnrichedAppNames(new EnrichedAppNames(
                    new LinkedHashMap<>(),
                    new LinkedHashMap<>()));
        }

        @Test
        public void callsCacheWithMergedResults()
        {
            EnrichedAppNames mergedResults = createMergedAppNames();
            fetcher.setLastAppNameHashcode(mergedResults.hashCode() - 1);

            fetcher.run();

            verify(cacheMock).populateEnrichedAppNames(mergedResults);
        }

        @Test
        public void doesntCallCacheWhenResultDidntChange()
        {
            EnrichedAppNames mergedResults = createMergedAppNames();
            fetcher.setLastAppNameHashcode(mergedResults.hashCode());

            fetcher.run();

            verify(cacheMock, never()).populateEnrichedAppNames(any());
        }
    }
}
