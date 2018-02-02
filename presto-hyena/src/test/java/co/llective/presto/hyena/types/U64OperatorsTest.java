/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package co.llective.presto.hyena.types;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
