package co.llective.presto.hyena.enrich.geoip;

import co.llective.presto.hyena.enrich.rest.RestClient;
import co.llective.presto.hyena.enrich.rest.RestClientException;
import co.llective.presto.hyena.enrich.username.CacheException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.List;

public class GeoIpFetcher
        implements Runnable
{
    private static final Logger log = Logger.get(GeoIpFetcher.class);
    //TODO: change this to /config/presto or /config/de when we will be dropping drill
    static final String TOUCAN_GEO_IP_ENDPOINT = "http://toucan:3000/config/drill/geoip_enrichment_user";

    private final GeoIpCache geoIpCache;
    private final RestClient restClient;
    private final ObjectMapper objectMapper;

    private int lastLocalGeoIpHashcode;

    @VisibleForTesting
    GeoIpFetcher(GeoIpCache geoIpCache, RestClient restClient, ObjectMapper objectMapper)
    {
        this.geoIpCache = geoIpCache;
        this.restClient = restClient;
        this.objectMapper = objectMapper;
    }

    GeoIpFetcher(GeoIpCache geoIpCache)
    {
        this(geoIpCache, new RestClient(), new ObjectMapper());
    }

    @Override
    public void run()
    {
        try {
            String localGeoIpJson = fetchLocalGeoIpJson();
            int responseHashcode = localGeoIpJson.hashCode();
            if (responseHashcode == lastLocalGeoIpHashcode) {
                log.debug("No new data for local geoip enrichment");
                return;
            }
            List<LocalGeoIpEnrichment> geoIpEnrichments = deserializeResponseJson(localGeoIpJson);
            lastLocalGeoIpHashcode = responseHashcode;
            geoIpCache.populateLocalGeoIp(geoIpEnrichments);
        }
        catch (Exception exc) {
            log.error("Local GeoIp info from toucan couldn't be populated");
        }
    }

    private String fetchLocalGeoIpJson()
            throws CacheException
    {
        try {
            return restClient.getJson(TOUCAN_GEO_IP_ENDPOINT);
        }
        catch (RestClientException exc) {
            throw new CacheException("Error during fetching local geoip data", exc);
        }
    }

    private List<LocalGeoIpEnrichment> deserializeResponseJson(String json)
            throws CacheException
    {
        try {
            LocalGeoIpEnrichment.LocalGeoIpEnrichments geoIpEnrichemnts =
                    objectMapper.readValue(json, LocalGeoIpEnrichment.LocalGeoIpEnrichments.class);
            return geoIpEnrichemnts.getEnrichedGeoIps();
        }
        catch (IOException exc) {
            throw new CacheException("Couldn't deserialize response", exc);
        }
    }

    @VisibleForTesting
    void setLastLocalGeoIpHashcode(int lastLocalGeoIpHashcode)
    {
        this.lastLocalGeoIpHashcode = lastLocalGeoIpHashcode;
    }

    @VisibleForTesting
    int getLastLocalGeoIpHashcode()
    {
        return lastLocalGeoIpHashcode;
    }
}
