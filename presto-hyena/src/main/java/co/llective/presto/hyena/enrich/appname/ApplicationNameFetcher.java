package co.llective.presto.hyena.enrich.appname;

import co.llective.presto.hyena.enrich.rest.RestClient;
import co.llective.presto.hyena.enrich.rest.RestClientException;
import co.llective.presto.hyena.enrich.username.CacheException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class ApplicationNameFetcher
        implements Runnable
{
    private static final Logger log = Logger.get(ApplicationNameFetcher.class);
    private static final String TOUCAN_ENDPOINT = "http://toucan:3000/config/";
    private static final String TOUCAN_APP_GLOBAL_ENDPOINT = TOUCAN_ENDPOINT + "drill/app_enrichment_global";
    private static final String TOUCAN_APP_USER_ENDPOINT = TOUCAN_ENDPOINT + "drill/app_enrichment_user";

    private final ApplicationNameCache cache;
    private final RestClient restClient;
    private final ObjectMapper objectMapper;

    private int lastAppNameHashcode;

    @VisibleForTesting
    ApplicationNameFetcher(ApplicationNameCache applicationNameCache, RestClient restClient, ObjectMapper objectMapper)
    {
        this.cache = applicationNameCache;
        this.restClient = restClient;
        this.objectMapper = objectMapper;
    }

    ApplicationNameFetcher(ApplicationNameCache applicationNameCache)
    {
        this(applicationNameCache, new RestClient(), new ObjectMapper());
    }


    @Override
    public void run()
    {
        EnrichedAppNames[] enrichedAppNamesList = new EnrichedAppNames[] {
                fetchAppNames(TOUCAN_APP_USER_ENDPOINT),
                fetchAppNames(TOUCAN_APP_GLOBAL_ENDPOINT)
        };

        EnrichedAppNames mergedAppNames = mergeAppNames(enrichedAppNamesList);
        int currentHashCode = mergedAppNames.hashCode();

        if (lastAppNameHashcode == currentHashCode) {
            log.debug("No new data for application name enrichment");
        } else {
            lastAppNameHashcode = mergedAppNames.hashCode();
            cache.populateEnrichedAppNames(mergedAppNames);
        }
    }

    private EnrichedAppNames fetchAppNames(String endpoint)
    {
        try {
            String appNamesJson = fetchAppNamesJson(endpoint);
            return objectMapper.readValue(appNamesJson, EnrichedAppNames.class);
        }
        catch (CacheException | IOException exc) {
            log.error("Problem with application names value update " + exc.getMessage(), exc);
            return null;
        }
    }

    private String fetchAppNamesJson(String endpoint)
            throws CacheException
    {
        try {
            return restClient.getJson(endpoint);
        }
        catch (RestClientException exc) {
            throw new CacheException("Error during fetching application names from " + endpoint, exc);
        }
    }

    private EnrichedAppNames mergeAppNames(EnrichedAppNames[] appNamesList) {
        LinkedHashMap<String, String> names = new LinkedHashMap<>();
        LinkedHashMap<Integer, String> ports = new LinkedHashMap<>();
        EnrichedAppNames result = new EnrichedAppNames(names, ports);

        for (EnrichedAppNames appNames : appNamesList) {
            if (appNames == null) {
                continue;
            }

            Map<String, String> newNames = appNames.getNames();
            if (newNames != null) {
                newNames.forEach(names::putIfAbsent);
            }

            Map<Integer, String> newPorts = appNames.getPorts();
            if (newPorts != null) {
                newPorts.forEach(ports::putIfAbsent);
            }
        }

        return result;
    }
}
