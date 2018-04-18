package co.llective.presto.hyena.enrich.geoip;

import co.llective.presto.hyena.enrich.util.IpUtil;
import co.llective.presto.hyena.enrich.util.SoftCache;
import co.llective.presto.hyena.enrich.util.SubnetV4;
import co.llective.presto.hyena.enrich.util.SubnetV6;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class GeoIpCacheUnitTest
{
    public static class GetValue
    {
        GeoIpCache cache;
        String cacheIp = "127.0.0.1";
        String geoDbIp = "127.0.0.2";
        String localIp4ProviderIp = "128.0.0.3";
        String nonExistingIp = "222.222.222.222";
        GeoIpCache.ResultProvider<String> geoDbIpProvider = (inet) -> {
            if (inet.getHostAddress().equals(geoDbIp)) {
                return geoDbIp;
            }
            else {
                throw new GeoIp2Exception("");
            }
        };
        SoftCache<String> geoIpCache;
        Map<SubnetV4, String> localIp4Provider;
        Map<SubnetV6, String> localIp6Provider;

        @BeforeMethod
        public void setUp()
        {
            cache = GeoIpCache.getInstance();

            localIp4Provider = new HashMap<>();
            SubnetV4 subnetV4 = new SubnetV4(localIp4ProviderIp, 24);
            localIp4Provider.put(subnetV4, localIp4ProviderIp);

            localIp6Provider = new HashMap<>();

            geoIpCache = new SoftCache<>();
            IpUtil.IpPair ipPair = IpUtil.parseIp(cacheIp);
            geoIpCache.getInnerMap(ipPair.getHighBits()).put(ipPair.getLowBits(), cacheIp);
        }

        @Test
        public void returnsCacheValueIfExist()
        {
            IpUtil.IpPair ipPair = IpUtil.parseIp(cacheIp);
            String value = cache.getValue(
                    ipPair.getHighBits(),
                    ipPair.getLowBits(),
                    geoDbIpProvider,
                    geoIpCache,
                    localIp4Provider,
                    localIp6Provider);

            assertEquals(value, cacheIp);
        }

        @Test
        public void returnsLocalSubnetsValueIfCacheDoesntExist()
        {
            IpUtil.IpPair ipPair = IpUtil.parseIp(localIp4ProviderIp);

            //assert that cache doesn't have entry before execution
            assertFalse(geoIpCache.getInnerMap(ipPair.getHighBits()).containsKey(ipPair.getLowBits()));

            String value = cache.getValue(
                    ipPair.getHighBits(),
                    ipPair.getLowBits(),
                    geoDbIpProvider,
                    geoIpCache,
                    localIp4Provider,
                    localIp6Provider);

            assertEquals(value, localIp4ProviderIp);

            //assert that cache had been populated with local ip
            assertTrue(geoIpCache.getInnerMap(ipPair.getHighBits()).containsKey(ipPair.getLowBits()));
            assertEquals(geoIpCache.getInnerMap(ipPair.getHighBits()).get(ipPair.getLowBits()), localIp4ProviderIp);
        }

        @Test
        public void returnsMaxmindProviderValueIfCacheAndLocalDoesntExist()
        {
            IpUtil.IpPair ipPair = IpUtil.parseIp(geoDbIp);

            //assert that cache doesn't have entry before execution
            assertFalse(geoIpCache.getInnerMap(ipPair.getHighBits()).containsKey(ipPair.getLowBits()));

            String value = cache.getValue(
                    ipPair.getHighBits(),
                    ipPair.getLowBits(),
                    geoDbIpProvider,
                    geoIpCache,
                    localIp4Provider,
                    localIp6Provider);

            assertEquals(value, geoDbIp);

            //assert that cache had been populated with local ip
            assertTrue(geoIpCache.getInnerMap(ipPair.getHighBits()).containsKey(ipPair.getLowBits()));
            assertEquals(geoIpCache.getInnerMap(ipPair.getHighBits()).get(ipPair.getLowBits()), geoDbIp);
        }

        @Test
        public void populatesCacheWithEmptyEntryWhenValueIsNotKnown()
        {
            IpUtil.IpPair ipPair = IpUtil.parseIp(nonExistingIp);

            //assert that cache doesn't have entry before execution
            assertFalse(geoIpCache.getInnerMap(ipPair.getHighBits()).containsKey(ipPair.getLowBits()));

            String value = cache.getValue(
                    ipPair.getHighBits(),
                    ipPair.getLowBits(),
                    geoDbIpProvider,
                    geoIpCache,
                    localIp4Provider,
                    localIp6Provider);

            assertNull(value);

            //assert that cache had been populated with local ip
            assertTrue(geoIpCache.getInnerMap(ipPair.getHighBits()).containsKey(ipPair.getLowBits()));
            assertNull(geoIpCache.getInnerMap(ipPair.getHighBits()).get(ipPair.getLowBits()));
        }
    }
}
