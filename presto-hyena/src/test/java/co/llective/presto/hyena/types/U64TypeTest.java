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

import com.google.common.primitives.UnsignedLong;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class U64TypeTest
{
    public static class CompareUnsignedLongs
    {
        @Test
        public void comparesCorrectlyPositiveNumbers()
        {
            long biggerUnsignedNumber = 0x00001000;
            long smallerUnsignedNumber = 0x00000001;

            int result = U64Type.U_64_TYPE.compareUnsignedLongs(biggerUnsignedNumber, smallerUnsignedNumber);
            assertEquals(result, 1);
        }

        @Test
        public void comparesCorrectlyPseudoNegativeNumbers()
        {
            // java uses two's complement ints
            Long biggerUnsignedNumber = 0x8000000000000002L;    // -9223372036854775806
            Long smallerUnsignedNumber = 0x8000000000000001L;   // -9223372036854775807

            assertTrue(biggerUnsignedNumber < 0);
            assertTrue(smallerUnsignedNumber < 0);

            int result = U64Type.U_64_TYPE.compareUnsignedLongs(biggerUnsignedNumber, smallerUnsignedNumber);
            assertEquals(result, 1);
            result = U64Type.U_64_TYPE.compareUnsignedLongs(smallerUnsignedNumber, biggerUnsignedNumber);
            assertEquals(result, -1);
        }

        @Test
        public void comparePositiveWithPseudoNegativeNumber()
        {
            Long pseudoNegativeNumber = 0x8000000000000000L;    // -2^63 signed or 2^64 unsigned
            Long positiveSignedNumber = 0x0000000000000001L;    // 1

            assertTrue(pseudoNegativeNumber < positiveSignedNumber);

            int signedComparisonResult = pseudoNegativeNumber.compareTo(positiveSignedNumber);
            assertEquals(signedComparisonResult, -1);

            int unsignedComparisonResult = U64Type.U_64_TYPE.compareUnsignedLongs(pseudoNegativeNumber, positiveSignedNumber);
            assertEquals(unsignedComparisonResult, 1);
            unsignedComparisonResult = U64Type.U_64_TYPE.compareUnsignedLongs(positiveSignedNumber, pseudoNegativeNumber);
            assertEquals(unsignedComparisonResult, -1);
        }

        @Test
        public void comparesEqualNumbers()
        {
            Long someNumber = 0x0000000000000500L;

            int result = U64Type.U_64_TYPE.compareUnsignedLongs(someNumber, someNumber);
            assertEquals(result, 0);
        }
    }

    public static class CompareToSignedLong
    {
        @Test
        public void pseudoNegativeToNegativeNumberReturnsGreaterThanZero()
        {
            Long u64 = 0x1000000000000500L;
            int result = U64Type.U_64_TYPE.compareToSignedLong(u64, -1);
            assertEquals(result, 1);
        }

        @Test
        public void positiveToNegativeNumberReturnsGreaterThanZero()
        {
            Long u64 = 0x0000000000000500L;
            int result = U64Type.U_64_TYPE.compareToSignedLong(u64, -1);
            assertEquals(result, 1);
        }

        @Test
        public void sameNumberReturnsZero()
        {
            int result = U64Type.U_64_TYPE.compareToSignedLong(10L, 10L);
            assertEquals(result, 0);
        }

        @Test
        public void comparingLesserNumberReturnsMinusOne()
        {
            int result = U64Type.U_64_TYPE.compareToSignedLong(10L, 15);
            assertEquals(result, -1);
        }

        @Test
        public void comparingGreaterNumberReturnsOne()
        {
            int result = U64Type.U_64_TYPE.compareToSignedLong(10L, 5);
            assertEquals(result, 1);
        }
    }

    public static class AddSignedInt
    {
        @Test
        public void addsPositiveNumber()
        {
            long u64 = 100L;
            int signed = 50;
            assertEquals(U64Type.U_64_TYPE.addSignedInt(u64, signed), 150L);
        }

        @Test
        public void addsNegativeNumber()
        {
            long u64 = 100L;
            int signed = -50;
            assertEquals(U64Type.U_64_TYPE.addSignedInt(u64, signed), 50L);
        }

        @Test
        public void addsCorrectlyToUnsignedValue()
        {
            int addend = 100000;
            long u64 = UnsignedLong.MAX_VALUE.minus(UnsignedLong.fromLongBits(addend * 2)).longValue();
            assertEquals(U64Type.U_64_TYPE.addSignedInt(u64, addend), UnsignedLong.MAX_VALUE.longValue() - addend);
        }

        @Test
        public void throwsArithmeticWhenAddingOverMax()
        {
            long u64 = UnsignedLong.MAX_VALUE.longValue();
            int signed = 1;
            assertThrows(() -> U64Type.U_64_TYPE.addSignedInt(u64, signed));
        }

        @Test
        public void throwsArithmeticWhenAddingNegativeUnderZero()
        {
            long u64 = UnsignedLong.ZERO.longValue();
            int signed = -1;
            assertThrows(() -> U64Type.U_64_TYPE.addSignedInt(u64, signed));
        }
    }

    public static class SubtractSignedInt
    {
        @Test
        public void subtractsPositiveNumber()
        {
            long u64 = 100L;
            int signed = 50;
            assertEquals(U64Type.U_64_TYPE.subtractSignedInt(u64, signed), 50L);
        }

        @Test
        public void subtractsNegativeNumber()
        {
            long u64 = 100L;
            int signed = -50;
            assertEquals(U64Type.U_64_TYPE.subtractSignedInt(u64, signed), 150L);
        }

        @Test
        public void subtractsCorrectlyToUnsignedValue()
        {
            int addend = 100000;
            long u64 = UnsignedLong.MAX_VALUE.longValue();
            assertEquals(U64Type.U_64_TYPE.subtractSignedInt(u64, addend), UnsignedLong.MAX_VALUE.minus(UnsignedLong.fromLongBits(addend)).longValue());
        }

        @Test
        public void throwsArithmeticWhenSubtractingUnderZero()
        {
            long u64 = UnsignedLong.ZERO.longValue();
            int signed = 1;
            assertThrows(() -> U64Type.U_64_TYPE.subtractSignedInt(u64, signed));
        }

        @Test
        public void throwsArithmeticWhenSubtractingNegativeOverMax()
        {
            long u64 = UnsignedLong.MAX_VALUE.longValue();
            int signed = -1;
            assertThrows(() -> U64Type.U_64_TYPE.subtractSignedInt(u64, signed));
        }
    }

    public static class MultiplySignedInt
    {
        @Test
        public void multiplyBySignedOneDontChangeResult()
        {
            long u64 = 100L;
            int signed = 1;
            assertEquals(U64Type.U_64_TYPE.multiplyBySignedInt(u64, signed), 100L);
        }

        @Test
        public void multiplyByUnsignedOneDontChangeResult()
        {
            long u64 = 1L;
            int signed = 100;
            assertEquals(U64Type.U_64_TYPE.multiplyBySignedInt(u64, signed), 100L);
        }

        @Test
        public void multiplyByPositiveNumber()
        {
            long u64 = 50L;
            int signed = 50;
            assertEquals(U64Type.U_64_TYPE.multiplyBySignedInt(u64, signed), 2500);
        }

        @Test
        public void throwsArithmeticWhenMultipliesByNegative()
        {
            long u64 = 50L;
            int signed = -50;
            assertThrows(() -> U64Type.U_64_TYPE.multiplyBySignedInt(u64, signed));
        }

        @Test
        public void throwsArithmeticWhenOverflow()
        {
            long u64 = UnsignedLong.MAX_VALUE.longValue();
            int signed = 2;
            assertThrows(() -> U64Type.U_64_TYPE.multiplyBySignedInt(u64, signed));
        }
    }

    public static class UnsignedMultiplyOverflows
    {
        @Test
        public void falseWhenZeros()
        {
            long maxLong = UnsignedLong.MAX_VALUE.longValue();
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(0, 0));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(0, maxLong));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(maxLong, 0));
        }
        @Test
        public void falseWhenOnes()
        {
            long maxLong = UnsignedLong.MAX_VALUE.longValue();
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(1, 1));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(maxLong, 1));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(1, maxLong));
        }

        @Test
        public void falseWhenNoOverflow()
        {
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(10, 50));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(100, 50));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(1000, 50));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(10000, 50));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(100000, 50));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(1000000, 50));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(10000000, 50));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(100000000, 50));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(1000000000, 50));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(10000000000L, 50));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(100000000000L, 50));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(1000000000000L, 50));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(10000000000000L, 50));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(100000000000000L, 50));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(1000000000000000L, 50));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(10000000000000000L, 50));
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(100000000000000000L, 50));

            long halfMax = UnsignedLong.MAX_VALUE.dividedBy(UnsignedLong.fromLongBits(2)).longValue();
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(halfMax, 2));
        }

        @Test
        public void notTrueWhenSignedOverflow()
        {
            assertFalse(U64Type.U_64_TYPE.unsignedMultiplyOverflows(Long.MAX_VALUE, 2));
        }

        @Test
        public void trueWhenOverflow()
        {
            long halfMax = UnsignedLong.MAX_VALUE.dividedBy(UnsignedLong.fromLongBits(2)).longValue();
            assertTrue(U64Type.U_64_TYPE.unsignedMultiplyOverflows(halfMax, 3));
        }
    }

    public static class DivideSignedInt
    {
        @Test
        public void divideByOneDoesntChangeDividend()
        {
            long u64 = 1234;
            assertEquals(U64Type.U_64_TYPE.divideBySignedInt(u64, 1), u64);
        }

        @Test
        public void dividesCorrectly()
        {
            long u64 = 21;
            int divider = 3;
            assertEquals(U64Type.U_64_TYPE.divideBySignedInt(u64, divider), 7);
        }

        @Test
        public void dividesUnsignedCorrectly()
        {
            long u64 = Long.MAX_VALUE + 1; // 9223372036854775808L
            long halfU64 = 4611686018427387904L;
            int divider = 2;
            assertEquals(U64Type.U_64_TYPE.divideBySignedInt(u64, divider), halfU64);
        }

        @Test
        public void throwsArithmeticWhenDivisionByZero()
        {
            assertThrows(() -> U64Type.U_64_TYPE.divideBySignedInt(10, 0));
        }

        @Test
        public void throwsArithmeticWhenDivisionByNegative()
        {
            assertThrows(() -> U64Type.U_64_TYPE.divideBySignedInt(10, -1));
        }
    }

    public static class ModuloSignedInt
    {
        @Test
        public void moduloByGreaterNumberReturnsOriginal()
        {
            long u64 = 1L;
            int divisor = 2;
            assertEquals(U64Type.U_64_TYPE.moduloSignedInt(u64, divisor), u64);
        }

        @Test
        public void moduloOnZeroReturnsZero()
        {
            long u64 = 0L;
            int divisor = 9;
            assertEquals(U64Type.U_64_TYPE.moduloSignedInt(u64, divisor), 0);
        }

        @Test
        public void moduloCorrectly()
        {
            long u64 = 5L;
            int divisor = 3;
            assertEquals(U64Type.U_64_TYPE.moduloSignedInt(u64, divisor), 2);
        }

        @Test
        public void moduloUnsignedCorrectly()
        {
            long u64 = Long.MAX_VALUE + 2; // 9223372036854775809L
            int divisor = 2;
            assertEquals(U64Type.U_64_TYPE.moduloSignedInt(u64, divisor), 1);
        }
    }
}
