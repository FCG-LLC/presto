package co.llective.presto.hyena.enrich.appname;

import co.llective.presto.hyena.enrich.util.IpUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;

import static co.llective.presto.hyena.enrich.appname.ApplicationNameCache.UNKNOWN_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class AppNameCacheUnitTest
{
    ApplicationNameCache cache;

    private static final String SUBNET_NAME = "sub1";
    private static final String SUBNET = "10.12.1.0/24";
    private static final Integer PORT = 80;
    private static final String PORT_NAME = "port80";

    private static final String CACHE_IP = "127.0.0.1";
    private static final String UNKNOWN_CACHE_IP = "127.0.0.2";
    private static final Integer PREPOPULATED_PORT = 20;

    private EnrichedAppNames createEnrichedNames()
    {
        LinkedHashMap<String, String> names = new LinkedHashMap<>();
        names.put(SUBNET, SUBNET_NAME);

        LinkedHashMap<Integer, String> ports = new LinkedHashMap<>();
        ports.put(PORT, PORT_NAME);

        return new EnrichedAppNames(names, ports);
    }

    @BeforeMethod
    public void setUp()
    {
        cache = ApplicationNameCache.getInstance();

        IpUtil.IpPair cachePair = IpUtil.parseIp(CACHE_IP);
        cache.getCache().put(cachePair.getHighBits(), cachePair.getLowBits(), CACHE_IP);

        IpUtil.IpPair unknownCachePair = IpUtil.parseIp(UNKNOWN_CACHE_IP);
        cache.getCache().put(unknownCachePair.getHighBits(), unknownCachePair.getLowBits(), UNKNOWN_NAME);

        cache.populateEnrichedAppNames(createEnrichedNames());

        cache.getPortNames()[PREPOPULATED_PORT] = PREPOPULATED_PORT.toString();
    }

    @Test
    public void returnsCacheValueIfExist()
    {
        IpUtil.IpPair ipPair = IpUtil.parseIp(CACHE_IP);

        cache.getApplicationName(ipPair.getHighBits(), ipPair.getLowBits());
    }

    @Test
    public void returnsPortValueIfUnknownIpInCache()
    {
        IpUtil.IpPair ipPair = IpUtil.parseIp(UNKNOWN_CACHE_IP);

        String name = cache.getApplicationName(
                ipPair.getHighBits(),
                ipPair.getLowBits(),
                PREPOPULATED_PORT);

        assertEquals(name, PREPOPULATED_PORT.toString());
    }

    @Test
    public void getsSubnetNameIfNotInCache()
    {
        String ip = "10.12.1.175";
        IpUtil.IpPair ipPair = IpUtil.parseIp(ip);

        String name = cache.getApplicationName(ipPair.getHighBits(), ipPair.getLowBits());

        assertEquals(name, SUBNET_NAME);

        //verify that internal cache was populated
        assertEquals(cache.getCache().get(ipPair.getHighBits(), ipPair.getLowBits()), SUBNET_NAME);
    }

    @Test
    public void populatesCacheWithUnknownWhenNotFound()
    {
        String ip = "8.8.8.8";
        IpUtil.IpPair ipPair = IpUtil.parseIp(ip);

        String name = cache.getApplicationName(ipPair.getHighBits(), ipPair.getLowBits());

        assertNull(name);

        //verify that internal cache was populated
        assertEquals(cache.getCache().get(ipPair.getHighBits(), ipPair.getLowBits()), UNKNOWN_NAME);
    }
}
