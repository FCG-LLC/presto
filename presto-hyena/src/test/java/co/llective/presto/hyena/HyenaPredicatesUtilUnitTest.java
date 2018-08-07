package co.llective.presto.hyena;

import co.llective.hyena.api.BlockType;
import co.llective.hyena.api.ScanComparison;
import co.llective.hyena.api.ScanOrFilters;
import co.llective.presto.hyena.types.U64Type;
import co.llective.presto.hyena.util.TimeBoundaries;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.primitives.UnsignedLong;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

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
            Optional<TimeBoundaries> tsConstraints = predicatesUtil.getTsConstraints(tupleDomain);
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
            Optional<TimeBoundaries> tsConstraints = predicatesUtil.getTsConstraints(tupleDomain);
            assertTrue(tsConstraints.isPresent());
            assertEquals(tsConstraints.get().getStart().longValue(), 1L);
            assertEquals(tsConstraints.get().getEnd().longValue(), 1L);
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
            Optional<TimeBoundaries> tsConstraints = predicatesUtil.getTsConstraints(tupleDomain);
            assertTrue(tsConstraints.isPresent());
            assertEquals(tsConstraints.get().getStart().longValue(), 10L);
            assertEquals(tsConstraints.get().getEnd().longValue(), 20L);
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
            Optional<TimeBoundaries> tsConstraints = predicatesUtil.getTsConstraints(tupleDomain);
            assertTrue(tsConstraints.isPresent());
            assertEquals(tsConstraints.get().getStart().longValue(), 10L);
            assertEquals(tsConstraints.get().getEnd().longValue(), 40L);
        }

        @Test
        public void returnsUnsignedMaxWhenNoRightBoundary()
        {
            Map<HyenaColumnHandle, Domain> domainMap = new HashMap<>();
            Range range1 = Range.greaterThanOrEqual(U64Type.U_64_TYPE, 10L);
            Domain domain = Domain.create(SortedRangeSet.copyOf(U64Type.U_64_TYPE, Collections.singletonList(range1)), false);
            domainMap.put(
                    new HyenaColumnHandle(
                            "timestamp",
                            U64Type.U_64_TYPE,
                            BlockType.U64Dense,
                            0),
                    domain);
            TupleDomain<HyenaColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);
            Optional<TimeBoundaries> tsConstraints = predicatesUtil.getTsConstraints(tupleDomain);
            assertTrue(tsConstraints.isPresent());
            assertEquals(tsConstraints.get().getStart().longValue(), 10L);
            assertEquals(tsConstraints.get().getEnd().longValue(), UnsignedLong.MAX_VALUE.longValue());
        }

        @Test
        public void returnsUnsignedZeroWhenNoLeftBoundary()
        {
            Map<HyenaColumnHandle, Domain> domainMap = new HashMap<>();
            Range range1 = Range.lessThanOrEqual(U64Type.U_64_TYPE, 10L);
            Domain domain = Domain.create(SortedRangeSet.copyOf(U64Type.U_64_TYPE, Collections.singletonList(range1)), false);
            domainMap.put(
                    new HyenaColumnHandle(
                            "timestamp",
                            U64Type.U_64_TYPE,
                            BlockType.U64Dense,
                            0),
                    domain);
            TupleDomain<HyenaColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);
            Optional<TimeBoundaries> tsConstraints = predicatesUtil.getTsConstraints(tupleDomain);
            assertTrue(tsConstraints.isPresent());
            assertEquals(tsConstraints.get().getStart().longValue(), UnsignedLong.ZERO.longValue());
            assertEquals(tsConstraints.get().getEnd().longValue(), 10L);
        }
    }

    public static class PredicateToFilters
    {
        HyenaPredicatesUtil predicatesUtil = new HyenaPredicatesUtil();

        @Test
        public void emptyPredicateEmptyFilters()
        {
            TupleDomain<HyenaColumnHandle> tupleDomain = TupleDomain.withColumnDomains(new HashMap<>());
            ScanOrFilters filters = predicatesUtil.predicateToFilters(tupleDomain);
            assertTrue(filters.isEmpty());
        }

        @Test
        public void oneDomainOneEqualRange()
        {
            Map<HyenaColumnHandle, Domain> domainMap = new HashMap<>();
            domainMap.put(
                    new HyenaColumnHandle(
                            "column",
                            U64Type.U_64_TYPE,
                            BlockType.U64Dense,
                            0),
                    Domain.singleValue(U64Type.U_64_TYPE, 1L));
            TupleDomain<HyenaColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);

            ScanOrFilters filters = predicatesUtil.predicateToFilters(tupleDomain);

            assertEquals(filters.size(), 1);
            assertEquals(filters.get(0).size(), 1);

            assertEquals(filters.get(0).get(0).getColumn(), 0);
            assertEquals(filters.get(0).get(0).getOp(), ScanComparison.Eq);
            assertEquals(filters.get(0).get(0).getValue(), 1L);
        }

        @Test
        public void oneDomainOneBetweenRange()
        {
            Map<HyenaColumnHandle, Domain> domainMap = new HashMap<>();
            Range range = Range.range(U64Type.U_64_TYPE, 10L, true, 20L, true);
            domainMap.put(
                    new HyenaColumnHandle(
                            "column",
                            U64Type.U_64_TYPE,
                            BlockType.U64Dense,
                            0),
                    Domain.create(SortedRangeSet.copyOf(U64Type.U_64_TYPE, Collections.singletonList(range)), false));
            TupleDomain<HyenaColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);

            ScanOrFilters filters = predicatesUtil.predicateToFilters(tupleDomain);

            assertEquals(filters.size(), 1);
            assertEquals(filters.get(0).size(), 2);

            assertEquals(filters.get(0).get(0).getColumn(), 0);
            assertEquals(filters.get(0).get(0).getOp(), ScanComparison.GtEq);
            assertEquals(filters.get(0).get(0).getValue(), 10L);

            assertEquals(filters.get(0).get(1).getColumn(), 0);
            assertEquals(filters.get(0).get(1).getOp(), ScanComparison.LtEq);
            assertEquals(filters.get(0).get(1).getValue(), 20L);
        }

        @Test
        public void twoDomainsSingleRanges()
        {
            Map<HyenaColumnHandle, Domain> domainMap = new HashMap<>();
            domainMap.put(
                    new HyenaColumnHandle(
                            "column1",
                            U64Type.U_64_TYPE,
                            BlockType.U64Dense,
                            0),
                    Domain.singleValue(U64Type.U_64_TYPE, 1L));
            domainMap.put(
                    new HyenaColumnHandle(
                            "column2",
                            VarcharType.VARCHAR,
                            BlockType.StringDense,
                            1),
                    Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice("value")));
            TupleDomain<HyenaColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);

            ScanOrFilters filters = predicatesUtil.predicateToFilters(tupleDomain);

            assertEquals(filters.size(), 1);
            assertEquals(filters.get(0).size(), 2);

            assertEquals(filters.get(0).get(0).getColumn(), 1);
            assertEquals(filters.get(0).get(0).getOp(), ScanComparison.Matches);
            assertEquals(filters.get(0).get(0).getValue(), "value");

            assertEquals(filters.get(0).get(1).getColumn(), 0);
            assertEquals(filters.get(0).get(1).getOp(), ScanComparison.Eq);
            assertEquals(filters.get(0).get(1).getValue(), 1L);
        }

        @Test
        public void multipleDomainsMultipleRanges()
        {
            Map<HyenaColumnHandle, Domain> domainMap = new HashMap<>();
            HyenaColumnHandle column1 = new HyenaColumnHandle(
                    "column1",
                    U64Type.U_64_TYPE,
                    BlockType.U64Dense,
                    0);
            HyenaColumnHandle column2 = new HyenaColumnHandle(
                    "column2",
                    VarcharType.VARCHAR,
                    BlockType.StringDense,
                    1);
            Range range1 = Range.range(U64Type.U_64_TYPE, 10L, true, 20L, true);
            Range range2 = Range.equal(U64Type.U_64_TYPE, 1L);
            domainMap.put(
                    column1,
                    Domain.create(SortedRangeSet.copyOf(U64Type.U_64_TYPE, Arrays.asList(range1, range2)), false));
            Range range3 = Range.equal(VarcharType.VARCHAR, Slices.utf8Slice("value1"));
            Range range4 = Range.equal(VarcharType.VARCHAR, Slices.utf8Slice(ENDS_WITH_CHAR + "value2"));
            domainMap.put(
                    column2,
                    Domain.create(SortedRangeSet.copyOf(VarcharType.VARCHAR, Arrays.asList(range3, range4)), false));
            TupleDomain<HyenaColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);

            ScanOrFilters filters = predicatesUtil.predicateToFilters(tupleDomain);

            // two ranges from first, two ranges from second -> 2x2 = 4 combinations
            assertEquals(filters.size(), 4);
            assertEquals(filters.get(0).size(), 2);
            assertEquals(filters.get(1).size(), 3);
            assertEquals(filters.get(2).size(), 2);
            assertEquals(filters.get(3).size(), 3);

            assertEquals(filters.get(0).get(0).getColumn(), 1);
            assertEquals(filters.get(0).get(0).getOp(), ScanComparison.EndsWith);
            assertEquals(filters.get(0).get(0).getValue(), "value2");
            assertEquals(filters.get(0).get(1).getColumn(), 0);
            assertEquals(filters.get(0).get(1).getOp(), ScanComparison.Eq);
            assertEquals(filters.get(0).get(1).getValue(), 1L);

            assertEquals(filters.get(1).get(0).getColumn(), 1);
            assertEquals(filters.get(1).get(0).getOp(), ScanComparison.EndsWith);
            assertEquals(filters.get(1).get(0).getValue(), "value2");
            assertEquals(filters.get(1).get(1).getColumn(), 0);
            assertEquals(filters.get(1).get(1).getOp(), ScanComparison.GtEq);
            assertEquals(filters.get(1).get(1).getValue(), 10L);
            assertEquals(filters.get(1).get(2).getColumn(), 0);
            assertEquals(filters.get(1).get(2).getOp(), ScanComparison.LtEq);
            assertEquals(filters.get(1).get(2).getValue(), 20L);

            assertEquals(filters.get(2).get(0).getColumn(), 1);
            assertEquals(filters.get(2).get(0).getOp(), ScanComparison.Matches);
            assertEquals(filters.get(2).get(0).getValue(), "value1");
            assertEquals(filters.get(2).get(1).getColumn(), 0);
            assertEquals(filters.get(2).get(1).getOp(), ScanComparison.Eq);
            assertEquals(filters.get(2).get(1).getValue(), 1L);

            assertEquals(filters.get(3).get(0).getColumn(), 1);
            assertEquals(filters.get(3).get(0).getOp(), ScanComparison.Matches);
            assertEquals(filters.get(3).get(0).getValue(), "value1");
            assertEquals(filters.get(3).get(1).getOp(), ScanComparison.GtEq);
            assertEquals(filters.get(3).get(1).getValue(), 10L);
            assertEquals(filters.get(3).get(2).getColumn(), 0);
            assertEquals(filters.get(3).get(2).getOp(), ScanComparison.LtEq);
            assertEquals(filters.get(3).get(2).getValue(), 20L);
        }

        @Test
        public void startEndWildcardedString()
        {
            Map<HyenaColumnHandle, Domain> domainMap = new HashMap<>();
            domainMap.put(
                    new HyenaColumnHandle(
                            "column",
                            VarcharType.VARCHAR,
                            BlockType.StringDense,
                            0),
                    Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice(CONTAINS_CHAR + "asd")));
            TupleDomain<HyenaColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);

            ScanOrFilters filters = predicatesUtil.predicateToFilters(tupleDomain);

            assertEquals(filters.size(), 1);
            assertEquals(filters.get(0).size(), 1);
            assertEquals(filters.get(0).get(0).getColumn(), 0);
            assertEquals(filters.get(0).get(0).getOp(), ScanComparison.Contains);
            assertEquals(filters.get(0).get(0).getValue(), "asd");
        }

        @Test
        public void startWildcardedString()
        {
            Map<HyenaColumnHandle, Domain> domainMap = new HashMap<>();
            domainMap.put(
                    new HyenaColumnHandle(
                            "column",
                            VarcharType.VARCHAR,
                            BlockType.StringDense,
                            0),
                    Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice(ENDS_WITH_CHAR + "asd")));
            TupleDomain<HyenaColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);

            ScanOrFilters filters = predicatesUtil.predicateToFilters(tupleDomain);

            assertEquals(filters.size(), 1);
            assertEquals(filters.get(0).size(), 1);
            assertEquals(filters.get(0).get(0).getColumn(), 0);
            assertEquals(filters.get(0).get(0).getOp(), ScanComparison.EndsWith);
            assertEquals(filters.get(0).get(0).getValue(), "asd");
        }

        private static final Character STARTS_WITH_CHAR = 0x11;
        private static final Character ENDS_WITH_CHAR = 0x12;
        private static final Character CONTAINS_CHAR = 0x13;

        @Test
        public void endWildcardedString()
        {
            Map<HyenaColumnHandle, Domain> domainMap = new HashMap<>();
            domainMap.put(
                    new HyenaColumnHandle(
                            "column",
                            VarcharType.VARCHAR,
                            BlockType.StringDense,
                            0),
                    Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice(STARTS_WITH_CHAR + "asd")));
            TupleDomain<HyenaColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);

            ScanOrFilters filters = predicatesUtil.predicateToFilters(tupleDomain);

            assertEquals(filters.size(), 1);
            assertEquals(filters.get(0).size(), 1);
            assertEquals(filters.get(0).get(0).getColumn(), 0);
            assertEquals(filters.get(0).get(0).getOp(), ScanComparison.StartsWith);
            assertEquals(filters.get(0).get(0).getValue(), "asd");
        }

        @Test
        public void noWildcardString()
        {
            Map<HyenaColumnHandle, Domain> domainMap = new HashMap<>();
            domainMap.put(
                    new HyenaColumnHandle(
                            "column",
                            VarcharType.VARCHAR,
                            BlockType.StringDense,
                            0),
                    Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice("asd")));
            TupleDomain<HyenaColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);

            ScanOrFilters filters = predicatesUtil.predicateToFilters(tupleDomain);

            assertEquals(filters.size(), 1);
            assertEquals(filters.get(0).size(), 1);
            assertEquals(filters.get(0).get(0).getColumn(), 0);
            assertEquals(filters.get(0).get(0).getOp(), ScanComparison.Matches);
            assertEquals(filters.get(0).get(0).getValue(), "asd");
        }
    }
}
