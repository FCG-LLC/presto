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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.AbstractFixedWidthType;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.primitives.UnsignedLong;

public final class U64Type
        extends AbstractFixedWidthType
{
    public static final U64Type U_64_TYPE = new U64Type();
    public static final String U_64_NAME = "unsigned_long";

    private U64Type()
    {
        super(TypeSignature.parseTypeSignature(U_64_NAME), long.class, 8);
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return compareTo(leftBlock, leftPosition, rightBlock, rightPosition) == 0;
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long leftValue = getLong(leftBlock, leftPosition);
        long rightValue = getLong(rightBlock, rightPosition);
        return compareUnsignedLongs(leftValue, rightValue);
    }

    public int compareUnsignedLongs(long leftValue, long rightValue)
    {
        return UnsignedLong.fromLongBits(leftValue).compareTo(UnsignedLong.fromLongBits(rightValue));
    }

    public int compareToSignedLong(long u64Value, long signedValue)
    {
        if (signedValue < 0) {
            return 1;
        }
        return compareUnsignedLongs(u64Value, signedValue);
    }

    public long addSignedInt(long u64, int signedInt)
    {
        long sum = u64 + signedInt;
        if ((signedInt < 0 && Long.compareUnsigned(sum, u64) > 0) ||
                (signedInt > 0 && Long.compareUnsigned(sum, u64) < 0)) {
            throw new ArithmeticException(java.lang.String.format(
                    "Unsigned addition overflow: %s + %s",
                    UnsignedLong.fromLongBits(u64),
                    signedInt));
        }
        return sum;
    }

    public long subtractSignedInt(long u64, int signedInt)
    {
        long difference = u64 - signedInt;
        if ((signedInt < 0 && Long.compareUnsigned(difference, u64) < 0) ||
                (signedInt > 0 && Long.compareUnsigned(difference, u64) > 0)) {
            throw new ArithmeticException(java.lang.String.format(
                    "Unsigned subtraction overflow: %s - %s",
                    UnsignedLong.fromLongBits(u64),
                    signedInt));
        }
        return difference;
    }

    public long multiplyBySignedInt(long u64, int signedInt)
    {
        if (signedInt < 0) {
            throw new ArithmeticException("Cannot do unsigned * negative_signed = unsigned");
        }
        if (unsignedMultiplyOverflows(u64, signedInt)) {
            throw new ArithmeticException(java.lang.String.format(
                    "Unsigned multiplication overflow: %s * %s",
                    UnsignedLong.fromLongBits(u64),
                    signedInt));
        }
        return u64 * signedInt;
    }

    // https://stackoverflow.com/a/20073469/2606175
    boolean unsignedMultiplyOverflows(final long a, final long b)
    {
        if ((a == 0L) || (b == 0L)) {
            // Unsigned overflow of a * b will not occur, since the result would be 0.
            return false;
        }
        if ((a == 1L) || (b == 1L)) {
            // Unsigned overflow of a * b will not occur, since the result would be a or b.
            return false;
        }
        if ((a < 0L) || (b < 0L)) {
            // Unsigned overflow of a * b will occur, since the highest bit of one argument is set, and a bit higher than the lowest bit of the other argument is set.
            return true;
        }
        /*
         * 1 < a <= Long.MAX_VALUE
         * 1 < b <= Long.MAX_VALUE
         *
         * Let n == Long.SIZE (> 2), the number of bits of the primitive representation.
         * Unsigned overflow of a * b will occur if and only if a * b >= 2^n.
         * Each side of the comparison must be re-written such that signed overflow will not occur:
         *
         *     [a.01]  a * b >= 2^n
         *     [a.02]  a * b > 2^n - 1
         *     [a.03]  a * b > ((2^(n-1) - 1) * 2) + 1
         *
         * Let M == Long.MAX_VALUE == 2^(n-1) - 1, and substitute:
         *
         *     [a.04]  a * b > (M * 2) + 1
         *
         * Assume the following identity for non-negative integer X and positive integer Y:
         *
         *     [b.01]  X == ((X / Y) * Y) + (X % Y)
         *
         * Let X == M and Y == b, and substitute:
         *
         *     [b.02]  M == ((M / b) * b) + (M % b)
         *
         * Substitute for M:
         *
         *     [a.04]  a * b > (M * 2) + 1
         *     [a.05]  a * b > ((((M / b) * b) + (M % b)) * 2) + 1
         *     [a.06]  a * b > ((M / b) * b * 2) + ((M % b) * 2) + 1
         *
         * Assume the following identity for non-negative integer X and positive integer Y:
         *
         *     [c.01]  X == ((X / Y) * Y) + (X % Y)
         *
         * Let X == ((M % b) * 2) + 1 and Y == b, and substitute:
         *
         *     [c.02]  ((M % b) * 2) + 1 == (((((M % b) * 2) + 1) / b) * b) + ((((M % b) * 2) + 1) % b)
         *
         * Substitute for ((M % b) * 2) + 1:
         *
         *     [a.06]  a * b > ((M / b) * b * 2) + ((M % b) * 2) + 1
         *     [a.07]  a * b > ((M / b) * b * 2) + (((((M % b) * 2) + 1) / b) * b) + ((((M % b) * 2) + 1) % b)
         *
         * Divide each side by b (// represents real division):
         *
         *     [a.08]  (a * b) // b > (((M / b) * b * 2) + (((((M % b) * 2) + 1) / b) * b) + ((((M % b) * 2) + 1) % b)) // b
         *     [a.09]  (a * b) // b > (((M / b) * b * 2) // b) + ((((((M % b) * 2) + 1) / b) * b) // b) + (((((M % b) * 2) + 1) % b) // b)
         *
         * Reduce each b-divided term that otherwise has a known factor of b:
         *
         *     [a.10]  a > ((M / b) * 2) + ((((M % b) * 2) + 1) / b) + (((((M % b) * 2) + 1) % b) // b)
         *
         * Let c == ((M % b) * 2) + 1), and substitute:
         *
         *     [a.11]  a > ((M / b) * 2) + (c / b) + ((c % b) // b)
         *
         * Assume the following tautology for integers X, Y and real Z such that 0 <= Z < 1:
         *
         *     [d.01]  X > Y + Z <==> X > Y
         *
         * Assume the following tautology for non-negative integer X and positive integer Y:
         *
         *     [e.01]  0 <= (X % Y) // Y < 1
         *
         * Let X == c and Y == b, and substitute:
         *
         *     [e.02]  0 <= (c % b) // b < 1
         *
         * Let X == a, Y == ((M / b) * 2) + (c / b), and Z == ((c % b) // b), and substitute:
         *
         *     [d.01]  X > Y + Z <==> X > Y
         *     [d.02]  a > ((M / b) * 2) + (c / b) + ((c % b) // b) <==> a > ((M / b) * 2) + (c / b)
         *
         * Drop the last term of the right-hand side:
         *
         *     [a.11]  a > ((M / b) * 2) + (c / b) + ((c % b) // b)
         *     [a.12]  a > ((M / b) * 2) + (c / b)
         *
         * Substitute for c:
         *
         *     [a.13]  a > ((M / b) * 2) + ((((M % b) * 2) + 1) / b)
         *
         * The first term of the right-hand side is clearly non-negative.
         * Determine the upper bound for the first term of the right-hand side (note that the least possible value of b == 2 produces the greatest possible value of (M / b) * 2):
         *
         *     [f.01]  (M / b) * 2 <= (M / 2) * 2
         *
         * Assume the following tautology for odd integer X:
         *
         *     [g.01]  (X / 2) * 2 == X - 1
         *
         * Let X == M and substitute:
         *
         *     [g.02]  (M / 2) * 2 == M - 1
         *
         * Substitute for (M / 2) * 2:
         *
         *     [f.01]  (M / b) * 2 <= (M / 2) * 2
         *     [f.02]  (M / b) * 2 <= M - 1
         *
         * The second term of the right-hand side is clearly non-negative.
         * Determine the upper bound for the second term of the right-hand side (note that the <= relation is preserved across positive integer division):
         *
         *     [h.01]  M % b < b
         *     [h.02]  M % b <= b - 1
         *     [h.03]  (M % b) * 2 <= (b - 1) * 2
         *     [h.04]  ((M % b) * 2) + 1 <= (b * 2) - 1
         *     [h.05]  (((M % b) * 2) + 1) / b <= ((b * 2) - 1) / b
         *     [h.06]  (((M % b) * 2) + 1) / b <= 1
         *
         * Since the upper bound of the first term is M - 1, and the upper bound of the second term is 1, the upper bound of the right-hand side is M.
         * Each side of the comparison has been re-written such that signed overflow will not occur.
         */
        return (a > ((Long.MAX_VALUE / b) * 2L) + ((((Long.MAX_VALUE % b) * 2L) + 1L) / b));
    }

    public long multiplyBySignedLong(long u64, long signedLong)
    {
        if (signedLong < 0) {
            throw new ArithmeticException("Cannot do unsigned * negative_signed = unsigned");
        }
        if (unsignedMultiplyOverflows(u64, signedLong)) {
            throw new ArithmeticException(java.lang.String.format(
                    "Unsigned multiplication overflow: %s * %s",
                    UnsignedLong.fromLongBits(u64),
                    signedLong));
        }
        return u64 * signedLong;
    }

    public long divideBySignedInt(long u64, int signedInt)
    {
        if (signedInt < 0) {
            throw new ArithmeticException("Cannot do unsigned / negative_signed = unsigned");
        }
        return Long.divideUnsigned(u64, signedInt);
    }

    public long divideBySignedLong(long u64, long signedLong)
    {
        if (signedLong < 0) {
            throw new ArithmeticException("Cannot do unsigned / negative_signed = unsigned");
        }
        return Long.divideUnsigned(u64, signedLong);
    }

    public long moduloSignedInt(long u64, int signedInt)
    {
        if (signedInt < 0) {
            signedInt = Math.abs(signedInt);
        }
        return Long.remainderUnsigned(u64, signedInt);
    }

    public long moduloSignedLong(long u64, long signedLong)
    {
        if (signedLong < 0) {
            signedLong = Math.abs(signedLong);
        }
        return Long.remainderUnsigned(u64, signedLong);
    }

    @Override
    public long hash(Block block, int position)
    {
        return block.hash(position, 0, getFixedSize());
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return getLong(block, position);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            writeLong(blockBuilder, block.getLong(position, 0));
        }
    }

    @Override
    public long getLong(Block block, int position)
    {
        return block.getLong(position, 0);
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        blockBuilder
                .writeLong(value)
                .closeEntry();
    }
}
