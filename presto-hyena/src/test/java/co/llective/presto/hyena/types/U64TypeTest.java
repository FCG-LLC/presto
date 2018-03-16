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

import static org.testng.Assert.assertEquals;
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

    public static class CompareToSignedLong {
        @Test
        public void pseudoNegativeToNegativeNumberReturnsGreaterThanZero()
        {
            Long u64 = 0x1000000000000500L;
            int result = U64Type.U_64_TYPE.compareToSignedLong(u64, -1);
            assertEquals(result, 1);
        }

        @Test
        public void positiveToNegativeNumberReturnsGreaterThanZero() {
            Long u64 = 0x0000000000000500L;
            int result = U64Type.U_64_TYPE.compareToSignedLong(u64, -1);
            assertEquals(result, 1);
        }

        @Test
        public void sameNumberReturnsZero() {
            int result = U64Type.U_64_TYPE.compareToSignedLong(10L, 10L);
            assertEquals(result, 0);
        }

        @Test
        public void comparingLesserNumberReturnsMinusOne() {
            int result = U64Type.U_64_TYPE.compareToSignedLong(10L, 15);
            assertEquals(result, -1);
        }

        @Test
        public void comparingGreaterNumberReturnsOne() {
            int result = U64Type.U_64_TYPE.compareToSignedLong(10L, 5);
            assertEquals(result, 1);
        }
    }
}
