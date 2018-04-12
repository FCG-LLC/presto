package co.llective.presto.hyena.enrich.username;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class UserTimedIpCacheUnitTest
{
    public static class Get
    {
        private static final Long IP_1 = 1L;
        private static final Long IP_2 = 2L;
        private static final Long TIMESTAMP = 3L;
        private static final String USER = "someUser";
        private static final Long NON_EXISTING_IP_1 = 5L;
        private static final Long NON_EXISTING_IP_2 = 6L;

        private UserTimedIpCache userTimedIpCache;

        @BeforeMethod
        public void setUp()
        {
            userTimedIpCache = new UserTimedIpCache();
            Map<Long, Map<Long, TimeUserCache>> internalCache = userTimedIpCache.getCache();

            TimeUserCache timeUserCache = mock(TimeUserCache.class);
            when(timeUserCache.getUser(TIMESTAMP)).thenReturn(USER);
            Map<Long, TimeUserCache> ip2TimeUserInternalCache = new HashMap<>();
            ip2TimeUserInternalCache.put(IP_2, timeUserCache);
            internalCache.put(IP_1, ip2TimeUserInternalCache);
        }

        @Test
        public void returnsNullIfNoIp1InCache()
        {
            String result = userTimedIpCache.get(NON_EXISTING_IP_1, IP_2, TIMESTAMP);
            assertNull(result);
        }

        @Test
        public void returnsNullIfNoIp2InCache()
        {
            String result = userTimedIpCache.get(IP_1, NON_EXISTING_IP_2, TIMESTAMP);
            assertNull(result);
        }

        @Test
        public void returnsCacheValueIfExists()
        {
            String result = userTimedIpCache.get(IP_1, IP_2, TIMESTAMP);
            assertEquals(USER, result);
        }
    }
}
