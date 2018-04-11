package co.llective.presto.enrich.username;

import io.airlift.log.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class UserNameCache
{
    private static final Logger log = Logger.get(UserNameCache.class);
    private static final ScheduledExecutorService SCHEDULED_THREAD = Executors.newSingleThreadScheduledExecutor();
    private static final int RELOAD_PERIOD_MIN = 5;

    private TimedIpCache<String> userNameCache = new UserTimedIpCache();

    private UserNameCache()
    {
        log.info("Scheduling user name enrichment updater. Reload period " + RELOAD_PERIOD_MIN + " minutes");
        SCHEDULED_THREAD.scheduleAtFixedRate(
                new UserCacheFetcher(this),
                0,
                RELOAD_PERIOD_MIN,
                TimeUnit.MINUTES);
    }

    private static class LazyHolder
    {
        static final UserNameCache INSTANCE = new UserNameCache();
    }

    static UserNameCache getInstance()
    {
        return LazyHolder.INSTANCE;
    }

    void populateEnrichedUsers(List<EnrichedUser> enrichedUsers)
    {
        userNameCache = new UserTimedIpCache();
        populateCache(userNameCache, enrichedUsers);
    }

    private void populateCache(TimedIpCache<String> cache, List<EnrichedUser> enrichedUsers)
    {
        // TODO: benchmark sorting
        enrichedUsers.sort(Comparator.comparing(EnrichedUser::getStartTs));

        for (EnrichedUser enrichedUser : enrichedUsers) {
            cache.put(
                    enrichedUser.getIp().getHighBits(),
                    enrichedUser.getIp().getLowBits(),
                    enrichedUser.getStartTs(),
                    enrichedUser.getEndTs(),
                    enrichedUser.getUser());
        }
    }

    String getUserName(long ip1, long ip2, long timestamp)
    {
        return userNameCache.get(ip1, ip2, timestamp);
    }
}
