package co.llective.presto.enrich.username;

import co.llective.presto.enrich.rest.RestClient;
import co.llective.presto.enrich.rest.RestClientException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.List;

public class UserCacheFetcher
        implements Runnable
{
    private static final Logger log = Logger.get(UserCacheFetcher.class);
//    private static final String DE_ENDPOINT = "http://data-enrichment:8888/ip-user";
    private static final String DE_ENDPOINT = "http://localhost:8888/ip-user";

    private final UserNameCache cache;
    private final RestClient restClient;
    private final ObjectMapper objectMapper;

    private int lastUserCacheHashcode;

    @VisibleForTesting
    UserCacheFetcher(UserNameCache cache, RestClient restClient, ObjectMapper objectMapper)
    {
        this.cache = cache;
        this.restClient = restClient;
        this.objectMapper = objectMapper;
    }

    UserCacheFetcher(UserNameCache cache)
    {
        this(cache, new RestClient(), new ObjectMapper());
    }

    @Override
    public void run()
    {
        try {
            //TODO: add logic for fetching only newest data (pass timestamp)
            String enrichedJson = fetchEnrichedJson();
            int enrichedHashcode = enrichedJson.hashCode();
            if (enrichedHashcode == lastUserCacheHashcode) {
                log.debug("No new data for enrichment");
                return;
            }
            List<EnrichedUser> enrichedUsers = deserializeResponseJson(enrichedJson);
            lastUserCacheHashcode = enrichedHashcode;
            log.debug("Populating " + enrichedUsers.size() + " users into cache");
            cache.populateEnrichedUsers(enrichedUsers);
        }
        catch (CacheException exc) {
            log.error("Cache couldn't be populated", exc.getMessage());
        }
    }

    private String fetchEnrichedJson() throws CacheException
    {
        return fetchEnrichedJson(null);
    }

    private String fetchEnrichedJson(Long timestamp) throws CacheException
    {
        try {
            String getParams = "";
            if (timestamp != null) {
                getParams = "?ts_from=" + timestamp;
            }
            return restClient.getJson(DE_ENDPOINT + getParams);
        }
        catch (RestClientException exc) {
            throw new CacheException("Error during fetching enrichment data", exc);
        }
    }

    private List<EnrichedUser> deserializeResponseJson(String json) throws CacheException
    {
        try {
            EnrichedUser.EnrichedUsers results = objectMapper.readValue(json, EnrichedUser.EnrichedUsers.class);
            return results.getEnrichedUsers();
        }
        catch (IOException exc) {
            throw new CacheException("Couldn't deserialize response", exc);
        }
    }

    @VisibleForTesting
    void setLastUserCacheHashcode(int lastUserCacheHashcode)
    {
        this.lastUserCacheHashcode = lastUserCacheHashcode;
    }

    @VisibleForTesting
    int getLastUserCacheHashcode()
    {
        return lastUserCacheHashcode;
    }
}
