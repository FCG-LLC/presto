package co.llective.presto.hyena.enrich.topdisco;

import co.llective.presto.hyena.enrich.util.SoftCache;
import io.airlift.log.Logger;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TopdiscoProvider
{
    private static final Logger log = Logger.get(TopdiscoProvider.class);
    private static final ScheduledExecutorService SCHEDULED_THREAD = Executors.newSingleThreadScheduledExecutor();
    private static final int RELOAD_PERIOD_MIN = 15;

    private final Map<Long, String> ipv4Names = new HashMap<>();
    private final Map<Long, Map<Long, String>> ipv6Names = new HashMap<>();
    private final Map<Long, String> ipv4RouterNames = new HashMap<>();
    private final Map<Long, Map<Long, String>> ipv6RouterNames = new HashMap<>();
    private final Map<Long, Map<Integer, String>> ipv4InterfaceNames = new HashMap<>();
    private final Map<Long, Map<Long, Map<Integer, String>>> ipv6InterfaceNames = new HashMap<>();
    private final SoftCache<String> ipStrings = new SoftCache<>();
    private SoftReference<Map<Integer, String>> interfaceNamesCache = new SoftReference<>(null);

    private TopdiscoProvider()
    {
        log.info("Scheduling topdisco enrichment data updated. Reload period " + RELOAD_PERIOD_MIN);
        SCHEDULED_THREAD.scheduleAtFixedRate(
                new TopdiscoFetcher(this),
                0,
                RELOAD_PERIOD_MIN,
                TimeUnit.MINUTES
        );
    }

    void populateTopdiscoData(TopdiscoEnrichment deserializedResponse)
    {
        //TODO
    }

    private static class LazyHolder
    {
        static final TopdiscoProvider INSTANCE = new TopdiscoProvider();
    }

    static TopdiscoProvider getInstance()
    {
        return LazyHolder.INSTANCE;
    }

    void clear() {
        ipv4Names.clear();
        ipv6Names.clear();
        ipv4RouterNames.clear();
        ipv6RouterNames.clear();
        ipv4InterfaceNames.clear();
        ipv6InterfaceNames.clear();
    }

    String getRouterName(long ip1, long ip2)
    {
        return null;
    }

    String getApplicationName(long ip1, long ip2, long interfaceNo)
    {
        return null;
    }
}
