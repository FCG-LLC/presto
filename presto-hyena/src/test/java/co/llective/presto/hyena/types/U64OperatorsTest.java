package co.llective.presto.hyena.types;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class U64OperatorsTest
{
    public static class LessThanBigInt
    {
        @Test
        public void returnsFalseWhenNegativeLongCloseToZero()
        {
            long u64 = 5L;
            long longNumber = -1L;
            assertFalse(U64BigIntOperators.lessThanBigInt(u64, longNumber));
        }

        @Test
        public void returnsFalseWhenNegativeLongMin()
        {
            long u64 = 5L;
            long longNumber = Long.MIN_VALUE;
            assertFalse(U64BigIntOperators.lessThanBigInt(u64, longNumber));
        }

        @Test
        public void returnsTrueIfu64smaller()
        {
            long u64 = 10L;
            long longNumber = 10000L;
            assertTrue(U64BigIntOperators.lessThanBigInt(u64, longNumber));
        }

        @Test
        public void returnsFalseIfu64bigger()
        {
            long u64 = 10000L;
            long longNumber = 10L;
            assertFalse(U64BigIntOperators.lessThanBigInt(u64, longNumber));
        }

        @Test
        public void returnsFalseWhenSameNumbers()
        {
            long u64 = 10L;
            assertFalse(U64BigIntOperators.lessThanBigInt(u64, u64));
        }
    }

    public static class LessThanOrEqualBigInt
    {
        @Test
        public void returnsFalseWhenNegativeLongCloseToZero()
        {
            long u64 = 5L;
            long longNumber = -1L;
            assertFalse(U64BigIntOperators.lessThanOrEqualBigInt(u64, longNumber));
        }

        @Test
        public void returnsFalseWhenNegativeLongMin()
        {
            long u64 = 5L;
            long longNumber = Long.MIN_VALUE;
            assertFalse(U64BigIntOperators.lessThanOrEqualBigInt(u64, longNumber));
        }

        @Test
        public void returnsTrueIfu64smaller()
        {
            long u64 = 10L;
            long longNumber = 10000L;
            assertTrue(U64BigIntOperators.lessThanOrEqualBigInt(u64, longNumber));
        }

        @Test
        public void returnsFalseIfu64bigger()
        {
            long u64 = 10000L;
            long longNumber = 10L;
            assertFalse(U64BigIntOperators.lessThanOrEqualBigInt(u64, longNumber));
        }

        @Test
        public void returnsTrueWhenSameNumbers()
        {
            long u64 = 10L;
            assertTrue(U64BigIntOperators.lessThanOrEqualBigInt(u64, u64));
        }
    }

    public static class GreaterThanBigInt
    {
        @Test
        public void returnsTrueWhenNegativeLongCloseToZero()
        {
            long u64 = 5L;
            long longNumber = -1L;
            assertTrue(U64BigIntOperators.greaterThanBigInt(u64, longNumber));
        }

        @Test
        public void returnsTrueWhenNegativeLongMin()
        {
            long u64 = 5L;
            long longNumber = Long.MIN_VALUE;
            assertTrue(U64BigIntOperators.greaterThanBigInt(u64, longNumber));
        }

        @Test
        public void returnsFalseIfu64smaller()
        {
            long u64 = 10L;
            long longNumber = 10000L;
            assertFalse(U64BigIntOperators.greaterThanBigInt(u64, longNumber));
        }

        @Test
        public void returnsTrueIfu64bigger()
        {
            long u64 = 10000L;
            long longNumber = 10L;
            assertTrue(U64BigIntOperators.greaterThanBigInt(u64, longNumber));
        }

        @Test
        public void returnsFalseWhenSameNumbers()
        {
            long u64 = 10L;
            assertFalse(U64BigIntOperators.greaterThanBigInt(u64, u64));
        }
    }

    public static class GreaterOrEqualThanBigInt
    {
        @Test
        public void returnsTrueWhenNegativeLongCloseToZero()
        {
            long u64 = 5L;
            long longNumber = -1L;
            assertTrue(U64BigIntOperators.greaterThanOrEqualBigInt(u64, longNumber));
        }

        @Test
        public void returnsTrueWhenNegativeLongMin()
        {
            long u64 = 5L;
            long longNumber = Long.MIN_VALUE;
            assertTrue(U64BigIntOperators.greaterThanOrEqualBigInt(u64, longNumber));
        }

        @Test
        public void returnsFalseIfu64smaller()
        {
            long u64 = 10L;
            long longNumber = 10000L;
            assertFalse(U64BigIntOperators.greaterThanOrEqualBigInt(u64, longNumber));
        }

        @Test
        public void returnsTrueIfu64bigger()
        {
            long u64 = 10000L;
            long longNumber = 10L;
            assertTrue(U64BigIntOperators.greaterThanOrEqualBigInt(u64, longNumber));
        }

        @Test
        public void returnsTrueWhenSameNumbers()
        {
            long u64 = 10L;
            assertTrue(U64BigIntOperators.greaterThanOrEqualBigInt(u64, u64));
        }
    }
}
