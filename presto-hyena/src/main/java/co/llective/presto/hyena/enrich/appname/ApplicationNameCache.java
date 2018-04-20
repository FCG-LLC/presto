package co.llective.presto.hyena.enrich.appname;

import co.llective.presto.hyena.enrich.util.IpUtil;
import co.llective.presto.hyena.enrich.util.SoftCache;
import co.llective.presto.hyena.enrich.util.SubnetV4;
import co.llective.presto.hyena.enrich.util.SubnetV6;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ApplicationNameCache
{
    private static final Logger log = Logger.get(ApplicationNameCache.class);
    private static final ScheduledExecutorService SCHEDULED_THREAD = Executors.newSingleThreadScheduledExecutor();
    private static final int RELOAD_PERIOD_MIN = 5;
    @VisibleForTesting static final String UNKNOWN_NAME = ""; // empty string is marking ip in cache as not named

    private Map<SubnetV4, String> ipv4Subnets = new LinkedHashMap<>();
    private Map<SubnetV6, String> ipv6Subnets = new LinkedHashMap<>();
    private String[] portNames = initPortNames();
    private final SoftCache<String> cache = new SoftCache<>();

    private ApplicationNameCache()
    {
        log.info("Scheduling local geoip enrichment updater. Reload period " + RELOAD_PERIOD_MIN + " minutes");
        SCHEDULED_THREAD.scheduleAtFixedRate(
                new ApplicationNameFetcher(this),
                0,
                RELOAD_PERIOD_MIN,
                TimeUnit.MINUTES);
    }

    private static class LazyHolder
    {
        static final ApplicationNameCache INSTANCE = new ApplicationNameCache();
    }

    static ApplicationNameCache getInstance()
    {
        return LazyHolder.INSTANCE;
    }

    private String[] initPortNames()
    {
        return new String[49151]; // largest not ephemeral port number
    }

    private void clear()
    {
        ipv4Subnets.clear();
        ipv6Subnets.clear();
        portNames = initPortNames();
        cache.clear();
    }

    void populateEnrichedAppNames(EnrichedAppNames appNames)
    {
        log.info("Populate new app enrichment: " +
                appNames.getNames().size() + " names and " +
                appNames.getPorts().size() + " ports");
        clear();
        populateNames(appNames.getNames());
        populatePortNames(appNames.getPorts());
    }

    private void populateNames(Map<String, String> names)
    {
        for (Map.Entry<String, String> entry : names.entrySet()) {
            String subnet = entry.getKey();
            String applicationName = entry.getValue();

            int index = subnet.indexOf("/");

            String address = subnet.substring(0, index);
            int maskLength = Integer.parseInt(subnet.substring(index + 1));

            try {
                InetAddress inetAddress = InetAddress.getByName(address);

                if (inetAddress instanceof Inet4Address) {
                    SubnetV4 subnetV4 = new SubnetV4(address, maskLength);
                    ipv4Subnets.put(subnetV4, applicationName);
                }
                else if (inetAddress instanceof Inet6Address) {
                    SubnetV6 subnetV6 = new SubnetV6(address, maskLength);
                    ipv6Subnets.put(subnetV6, applicationName);
                }
                else {
                    throw new UnknownHostException();
                }
            }
            catch (UnknownHostException exc) {
                log.debug("Wrong IP address `" + address + "`", exc);
            }
        }
    }

    private void populatePortNames(Map<Integer, String> ports)
    {
        for (Map.Entry<Integer, String> entry : ports.entrySet()) {
            Integer port = entry.getKey();
            String applicationName = entry.getValue();
            portNames[port] = applicationName;
        }
    }

    String getApplicationName(long ip1, long ip2, long port)
    {
        String cacheValue = cache.get(ip1, ip2);
        if (cacheValue != null) {
            return cacheValue.equals(UNKNOWN_NAME) ? getPortName(port) : cacheValue;
        }

        if (ip1 == IpUtil.WKP) {
            for (Map.Entry<SubnetV4, String> entry : ipv4Subnets.entrySet()) {
                SubnetV4 subnet = entry.getKey();
                if ((subnet.getMask() & ip2) == subnet.getAddress()) {
                    cache.put(ip1, ip2, entry.getValue());
                    return entry.getValue();
                }
            }
        }
        else {
            for (Map.Entry<SubnetV6, String> entry : ipv6Subnets.entrySet()) {
                SubnetV6 subnet = entry.getKey();
                if ((subnet.getMaskHighBits() & ip1) == subnet.getAddressHighBits()
                        && (subnet.getMaskLowBits() & ip2) == subnet.getAddressLowBits()) {
                    cache.put(ip1, ip2, entry.getValue());
                    return entry.getValue();
                }
            }
        }

        cache.put(ip1, ip2, UNKNOWN_NAME);
        return getPortName(port);
    }

    String getApplicationName(long ip1, long ip2)
    {
        return getApplicationName(ip1, ip2, -1);
    }

    private String getPortName(long port)
    {
        return port > 0 && port < portNames.length ? portNames[(int) port] : null;
    }

    @VisibleForTesting
    SoftCache<String> getCache()
    {
        return cache;
    }

    @VisibleForTesting
    public String[] getPortNames()
    {
        return portNames;
    }
}
