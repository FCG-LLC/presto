package co.llective.presto.enrich.username;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TimeUserCacheUnitTest
{
    static final String USER_0 = "user0";
    static final String USER_1 = "user1";
    static final String USER_2 = "user2";

    public static class GetUser
    {
        TimeUserCache timeUserCache;

        @BeforeMethod
        public void setUp()
        {
            timeUserCache = new TimeUserCache();
        }

        @Test
        public void returnsNullIfNotPopulated()
        {
            assertNull(timeUserCache.getUser(1L));
        }

        private void populateCache()
        {
            List<Long> timestamps = timeUserCache.getTimestamps();
            List<String> users = timeUserCache.getUsers();
            timestamps.addAll(Arrays.asList(
                    100L, 120L, 140L, 180L, 250L));
            users.addAll(Arrays.asList(
                    USER_0, null, USER_1, USER_2, null));
        }

        @Test
        public void returnsCorrectNonNullUserForTimestamp()
        {
            populateCache();

            String actualUser = timeUserCache.getUser(100L);

            assertEquals(USER_0, actualUser);
        }

        @Test
        public void returnsCorrectNullUserForTimestampWhenNoUser()
        {
            populateCache();

            String actualUser = timeUserCache.getUser(120L);

            assertNull(actualUser);
        }

        @Test
        public void returnsNullUserForTimestampAfterLastAdded()
        {
            populateCache();

            String actualUser = timeUserCache.getUser(300L);

            assertNull(actualUser);
        }
    }

    public static class AddNextUser
    {
        private static final String NEW_USER = "someUser";
        TimeUserCache timeUserCache;

        @BeforeMethod
        public void setUp()
        {
            timeUserCache = new TimeUserCache();
        }

        private List<Long> getCacheTimestamps()
        {
            return timeUserCache.getTimestamps();
        }

        private List<String> getCacheUsers()
        {
            return timeUserCache.getUsers();
        }

        private void populateCache()
        {
            getCacheTimestamps().addAll(Arrays.asList(
                    100L, 120L, 140L, 180L, 250L));
            getCacheUsers().addAll(Arrays.asList(
                    USER_0, null, USER_1, USER_2, null));
        }

        @Test
        public void initializesEmptyCache()
        {
            assertTrue(getCacheTimestamps().isEmpty());
            assertTrue(getCacheUsers().isEmpty());
        }

        @Test(expectedExceptions = IllegalArgumentException.class)
        public void throwsWhenAddingNotToTheEnd()
        {
            populateCache();
            timeUserCache.addNextUser(80L, 110L, NEW_USER);
        }

        @Test
        public void addsMiniBucketWhenWiderWithSameStartTsProvided()
        {
            populateCache();
            timeUserCache.addNextUser(180L, 270L, NEW_USER);

            assertEquals(
                    Arrays.asList(100L, 120L, 140L, 180L, 250L, 270L),
                    getCacheTimestamps());
            assertEquals(
                    Arrays.asList(USER_0, null, USER_1, USER_2, NEW_USER, null),
                    getCacheUsers());
        }

        @Test
        public void updatesBucketWhenShorterWithSameStartTsProvided()
        {
            populateCache();
            timeUserCache.addNextUser(180L, 230L, NEW_USER);

            assertEquals(
                    Arrays.asList(100L, 120L, 140L, 180L, 230L),
                    getCacheTimestamps());
            assertEquals(
                    Arrays.asList(USER_0, null, USER_1, NEW_USER, null),
                    getCacheUsers());
        }

        @Test
        public void splitsBucketWhenNewerStartTsProvided()
        {
            populateCache();
            timeUserCache.addNextUser(190L, 230L, NEW_USER);

            assertEquals(
                    Arrays.asList(100L, 120L, 140L, 180L, 190L, 230L),
                    getCacheTimestamps());
            assertEquals(
                    Arrays.asList(USER_0, null, USER_1, USER_2, NEW_USER, null),
                    getCacheUsers());
        }

        @Test
        public void addsNextEntryWhenTimeRangeCatchOnLastOne()
        {
            populateCache();
            timeUserCache.addNextUser(250L, 280L, NEW_USER);

            assertEquals(
                    Arrays.asList(100L, 120L, 140L, 180L, 250L, 280L),
                    getCacheTimestamps());
            assertEquals(
                    Arrays.asList(USER_0, null, USER_1, USER_2, NEW_USER, null),
                    getCacheUsers());
        }

        @Test
        public void addsNextEntryWhenTimeRangeIsWithDelayToLastOne()
        {
            populateCache();
            timeUserCache.addNextUser(260L, 280L, NEW_USER);

            assertEquals(
                    Arrays.asList(100L, 120L, 140L, 180L, 250L, 260L, 280L),
                    getCacheTimestamps());
            assertEquals(
                    Arrays.asList(USER_0, null, USER_1, USER_2, null, NEW_USER, null),
                    getCacheUsers());
        }
    }

    private TimeUserCacheUnitTest() {}
}
