package co.llective.presto.hyena.types;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class U64TypeTest
{
    public static class CompareUnsignedLongs {
        @Test
        public void comparesCorrectlyPositiveNumbers() {
            long biggerUnsignedNumber = 0x00001000;
            long smallerUnsignedNumber = 0x00000001;

            int result = U64Type.U_64_TYPE.compareUnsignedLongs(biggerUnsignedNumber, smallerUnsignedNumber);
            assertEquals(result, 1);
        }

        @Test
        public void comparesCorrectlyPseudoNegativeNumbers() {
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
        public void comparePositiveWithPseudoNegativeNumber() {
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
        public void comparesEqualNumbers() {
            Long someNumber = 0x0000000000000500L;

            int result = U64Type.U_64_TYPE.compareUnsignedLongs(someNumber, someNumber);
            assertEquals(result, 0);
        }
    }
}
