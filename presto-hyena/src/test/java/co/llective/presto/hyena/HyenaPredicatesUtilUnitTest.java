package co.llective.presto.hyena;

import co.llective.hyena.api.BlockType;
import co.llective.presto.hyena.types.U64Type;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.TupleDomain;
import org.testng.annotations.Test;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class HyenaPredicatesUtilUnitTest
{
    public static class GetTsConstraints
    {
        HyenaPredicatesUtil predicatesUtil = new HyenaPredicatesUtil();

        @Test
        public void returnsEmptyWhenNoTimestamp()
        {
            TupleDomain<HyenaColumnHandle> tupleDomain = TupleDomain.withColumnDomains(new HashMap<>());
            Optional<AbstractMap.SimpleEntry<Long, Long>> tsConstraints = predicatesUtil.getTsConstraints(tupleDomain);
            assertFalse(tsConstraints.isPresent());
        }

        @Test
        public void returnsSameValueIfSingleValueDomain()
        {
            Map<HyenaColumnHandle, Domain> domainMap = new HashMap<>();
            domainMap.put(
                    new HyenaColumnHandle(
                            "timestamp",
                            U64Type.U_64_TYPE,
                            BlockType.U64Dense,
                            0),
                    Domain.singleValue(U64Type.U_64_TYPE, 1L));
            TupleDomain<HyenaColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);
            Optional<AbstractMap.SimpleEntry<Long, Long>> tsConstraints = predicatesUtil.getTsConstraints(tupleDomain);
            assertTrue(tsConstraints.isPresent());
            assertEquals(tsConstraints.get().getKey().longValue(), 1L);
            assertEquals(tsConstraints.get().getValue().longValue(), 1L);
        }

        @Test
        public void returnsRangeFromDomain()
        {
            Map<HyenaColumnHandle, Domain> domainMap = new HashMap<>();
            Range range = Range.range(U64Type.U_64_TYPE, 10L, true, 20L, true);
            Domain domain = Domain.create(SortedRangeSet.copyOf(U64Type.U_64_TYPE, Collections.singletonList(range)), false);
            domainMap.put(
                    new HyenaColumnHandle(
                            "timestamp",
                            U64Type.U_64_TYPE,
                            BlockType.U64Dense,
                            0),
                    domain);
            TupleDomain<HyenaColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);
            Optional<AbstractMap.SimpleEntry<Long, Long>> tsConstraints = predicatesUtil.getTsConstraints(tupleDomain);
            assertTrue(tsConstraints.isPresent());
            assertEquals(tsConstraints.get().getKey().longValue(), 10L);
            assertEquals(tsConstraints.get().getValue().longValue(), 20L);
        }

        @Test
        public void returnsMinMaxFromMultipleRanges()
        {
            Map<HyenaColumnHandle, Domain> domainMap = new HashMap<>();
            Range range1 = Range.range(U64Type.U_64_TYPE, 10L, true, 20L, true);
            Range range2 = Range.range(U64Type.U_64_TYPE, 30L, true, 40L, true);
            Domain domain = Domain.create(SortedRangeSet.copyOf(U64Type.U_64_TYPE, Arrays.asList(range1, range2)), false);
            domainMap.put(
                    new HyenaColumnHandle(
                            "timestamp",
                            U64Type.U_64_TYPE,
                            BlockType.U64Dense,
                            0),
                    domain);
            TupleDomain<HyenaColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);
            Optional<AbstractMap.SimpleEntry<Long, Long>> tsConstraints = predicatesUtil.getTsConstraints(tupleDomain);
            assertTrue(tsConstraints.isPresent());
            assertEquals(tsConstraints.get().getKey().longValue(), 10L);
            assertEquals(tsConstraints.get().getValue().longValue(), 40L);
        }
    }
}
