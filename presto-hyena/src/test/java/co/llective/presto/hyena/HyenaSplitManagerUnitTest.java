package co.llective.presto.hyena;

import co.llective.presto.hyena.util.TimeBoundaries;
import com.facebook.presto.spi.NodeManager;
import com.google.common.primitives.UnsignedLong;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;

public class HyenaSplitManagerUnitTest
{
    public static class SplitTimeBoundaries
    {
        HyenaSplitManager splitManager;

        @BeforeTest
        public void setUp()
        {
            splitManager = spy(new HyenaSplitManager(mock(NodeManager.class), mock(HyenaSession.class)));
        }

        @Test
        public void splitsCorrectly()
        {
            int splitNo = 5;
            splitManager.numberOfSplits = splitNo;

            List<TimeBoundaries> expectedBoundaries = new ArrayList<>();

            expectedBoundaries.add(TimeBoundaries.of(0L, 200L));
            expectedBoundaries.add(TimeBoundaries.of(200L, 400L));
            expectedBoundaries.add(TimeBoundaries.of(400L, 600L));
            expectedBoundaries.add(TimeBoundaries.of(600L, 800L));
            expectedBoundaries.add(TimeBoundaries.of(800L, 1000L));

            TimeBoundaries timeBoundaries = TimeBoundaries.of(0L, 1000L);
            List<TimeBoundaries> result = splitManager.splitTimeBoundaries(timeBoundaries);

            assertEquals(result.size(), splitNo);

            assertEquals(result, expectedBoundaries);
        }

        @Test
        public void ifMinDoesntExistThenAddsZeroToDbMinTimestampRange()
        {
            int splitNo = 5;
            long dbMinTs = 10;
            splitManager.numberOfSplits = splitNo;
            splitManager.dbMinTimestamp = dbMinTs;

            List<TimeBoundaries> expectedBoundaries = new ArrayList<>();

            expectedBoundaries.add(TimeBoundaries.of(0L, dbMinTs));
            expectedBoundaries.add(TimeBoundaries.of(dbMinTs, 210L));
            expectedBoundaries.add(TimeBoundaries.of(210L, 410L));
            expectedBoundaries.add(TimeBoundaries.of(410L, 610L));
            expectedBoundaries.add(TimeBoundaries.of(610L, 810L));
            expectedBoundaries.add(TimeBoundaries.of(810L, 1010L));

            TimeBoundaries timeBoundaries = TimeBoundaries.of(null, 1010L);
            List<TimeBoundaries> result = splitManager.splitTimeBoundaries(timeBoundaries);

            assertEquals(result.size(), splitNo + 1);

            assertEquals(result, expectedBoundaries);
        }

        @Test
        public void ifMaxDoesntExistThenAddsArtMaxTimestampRange()
        {
            int splitNo = 5;
            long artMaxTs = 100;
            splitManager.numberOfSplits = splitNo;
            doReturn(artMaxTs).when(splitManager).getArtificialMaxTime();

            List<TimeBoundaries> expectedBoundaries = new ArrayList<>();

            expectedBoundaries.add(TimeBoundaries.of(artMaxTs, UnsignedLong.MAX_VALUE.longValue()));
            expectedBoundaries.add(TimeBoundaries.of(0L, 20L));
            expectedBoundaries.add(TimeBoundaries.of(20L, 40L));
            expectedBoundaries.add(TimeBoundaries.of(40L, 60L));
            expectedBoundaries.add(TimeBoundaries.of(60L, 80L));
            expectedBoundaries.add(TimeBoundaries.of(80L, 100L));

            TimeBoundaries timeBoundaries = TimeBoundaries.of(0L, null);
            List<TimeBoundaries> result = splitManager.splitTimeBoundaries(timeBoundaries);

            assertEquals(result.size(), splitNo + 1);

            assertEquals(result, expectedBoundaries);
        }

        @Test
        public void ifMinAndMaxDoesntExistThenSplitAndAddAdditionalRanges()
        {
            int splitNo = 5;
            long dbMinTs = 10;
            long artMaxTs = 110;
            splitManager.numberOfSplits = splitNo;
            splitManager.dbMinTimestamp = dbMinTs;
            doReturn(artMaxTs).when(splitManager).getArtificialMaxTime();

            List<TimeBoundaries> expectedBoundaries = new ArrayList<>();

            expectedBoundaries.add(TimeBoundaries.of(0L, dbMinTs));
            expectedBoundaries.add(TimeBoundaries.of(artMaxTs, UnsignedLong.MAX_VALUE.longValue()));
            expectedBoundaries.add(TimeBoundaries.of(dbMinTs, 30L));
            expectedBoundaries.add(TimeBoundaries.of(30L, 50L));
            expectedBoundaries.add(TimeBoundaries.of(50L, 70L));
            expectedBoundaries.add(TimeBoundaries.of(70L, 90L));
            expectedBoundaries.add(TimeBoundaries.of(90L, 110L));

            TimeBoundaries timeBoundaries = TimeBoundaries.of(null, null);
            List<TimeBoundaries> result = splitManager.splitTimeBoundaries(timeBoundaries);

            assertEquals(result.size(), splitNo + 2);

            assertEquals(result, expectedBoundaries);
        }
    }
}
