package co.llective.presto.enrich.username;

import co.llective.presto.enrich.rest.RestClient;
import co.llective.presto.enrich.rest.RestClientException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.List;

public class UserCacheFetcher
        implements Runnable
{
    private static final Logger log = Logger.get(UserCacheFetcher.class);
    private static final String DE_ENDPOINT = "http://data-enrichment:8888/ip-user";
    private final UserNameCache cache;
    private int lastUserCacheHashcode;

    UserCacheFetcher(UserNameCache cache)
    {
        this.cache = cache;
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
            lastUserCacheHashcode = enrichedHashcode;
            List<EnrichedUser> enrichedUsers = deserializeResponseJson(enrichedJson);
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
            RestClient restClient = new RestClient();
            return restClient.getJson(DE_ENDPOINT + getParams);
        }
        catch (RestClientException exc) {
            throw new CacheException("Error during fetching enrichment data", exc);
        }
    }

    private List<EnrichedUser> deserializeResponseJson(String json) throws CacheException
    {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            EnrichedUser.EnrichedUsers results = objectMapper.readValue(json, EnrichedUser.EnrichedUsers.class);
            return results.getEnrichedUsers();
        }
        catch (IOException exc) {
            throw new CacheException("Couldn't deserialize response", exc);
        }
    }
}
