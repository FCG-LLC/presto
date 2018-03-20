package co.llective.presto.hyena.types;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class U64IntOperatorsTest
{
    public static class LessThanInteger
    {
        @Test
        public void returnsFalseWhenNegativeLongCloseToZero()
        {
            long u64 = 5L;
            long intNumber = -1L;
            assertFalse(U64IntOperators.lessThanInteger(u64, intNumber));
        }

        @Test
        public void returnsFalseWhenNegativeLongMin()
        {
            long u64 = 5L;
            long intNumber = Integer.MIN_VALUE;
            assertFalse(U64IntOperators.lessThanInteger(u64, intNumber));
        }

        @Test
        public void returnsTrueIfu64smaller()
        {
            long u64 = 10L;
            long intNumber = 10000L;
            assertTrue(U64IntOperators.lessThanInteger(u64, intNumber));
        }

        @Test
        public void returnsFalseIfu64bigger()
        {
            long u64 = 10000L;
            long intNumber = 10L;
            assertFalse(U64IntOperators.lessThanInteger(u64, intNumber));
        }

        @Test
        public void returnsFalseWhenSameNumbers()
        {
            long u64 = 10L;
            assertFalse(U64IntOperators.lessThanInteger(u64, u64));
        }
    }

    public static class LessThanOrEqualInteger
    {
        @Test
        public void returnsFalseWhenNegativeLongCloseToZero()
        {
            long u64 = 5L;
            long intNumber = -1L;
            assertFalse(U64IntOperators.lessThanOrEqualInteger(u64, intNumber));
        }

        @Test
        public void returnsFalseWhenNegativeLongMin()
        {
            long u64 = 5L;
            long intNumber = Integer.MIN_VALUE;
            assertFalse(U64IntOperators.lessThanOrEqualInteger(u64, intNumber));
        }

        @Test
        public void returnsTrueIfu64smaller()
        {
            long u64 = 10L;
            long intNumber = 10000L;
            assertTrue(U64IntOperators.lessThanOrEqualInteger(u64, intNumber));
        }

        @Test
        public void returnsFalseIfu64bigger()
        {
            long u64 = 10000L;
            long intNumber = 10L;
            assertFalse(U64IntOperators.lessThanOrEqualInteger(u64, intNumber));
        }

        @Test
        public void returnsTrueWhenSameNumbers()
        {
            long u64 = 10L;
            assertTrue(U64IntOperators.lessThanOrEqualInteger(u64, u64));
        }
    }

    public static class GreaterThanInteger
    {
        @Test
        public void returnsTrueWhenNegativeLongCloseToZero()
        {
            long u64 = 5L;
            long intNumber = -1L;
            assertTrue(U64IntOperators.greaterThanInteger(u64, intNumber));
        }

        @Test
        public void returnsTrueWhenNegativeLongMin()
        {
            long u64 = 5L;
            long intNumber = Integer.MIN_VALUE;
            assertTrue(U64IntOperators.greaterThanInteger(u64, intNumber));
        }

        @Test
        public void returnsFalseIfu64smaller()
        {
            long u64 = 10L;
            long intNumber = 10000L;
            assertFalse(U64IntOperators.greaterThanInteger(u64, intNumber));
        }

        @Test
        public void returnsTrueIfu64bigger()
        {
            long u64 = 10000L;
            long intNumber = 10L;
            assertTrue(U64IntOperators.greaterThanInteger(u64, intNumber));
        }

        @Test
        public void returnsFalseWhenSameNumbers()
        {
            long u64 = 10L;
            assertFalse(U64IntOperators.greaterThanInteger(u64, u64));
        }
    }

    public static class GreaterOrEqualThanInteger
    {
        @Test
        public void returnsTrueWhenNegativeLongCloseToZero()
        {
            long u64 = 5L;
            long intNumber = -1L;
            assertTrue(U64IntOperators.greaterThanOrEqualInteger(u64, intNumber));
        }

        @Test
        public void returnsTrueWhenNegativeLongMin()
        {
            long u64 = 5L;
            long intNumber = Integer.MIN_VALUE;
            assertTrue(U64IntOperators.greaterThanOrEqualInteger(u64, intNumber));
        }

        @Test
        public void returnsFalseIfu64smaller()
        {
            long u64 = 10L;
            long intNumber = 10000L;
            assertFalse(U64IntOperators.greaterThanOrEqualInteger(u64, intNumber));
        }

        @Test
        public void returnsTrueIfu64bigger()
        {
            long u64 = 10000L;
            long intNumber = 10L;
            assertTrue(U64IntOperators.greaterThanOrEqualInteger(u64, intNumber));
        }

        @Test
        public void returnsTrueWhenSameNumbers()
        {
            long u64 = 10L;
            assertTrue(U64IntOperators.greaterThanOrEqualInteger(u64, u64));
        }
    }
}
