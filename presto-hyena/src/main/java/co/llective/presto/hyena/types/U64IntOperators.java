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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

import static co.llective.presto.hyena.types.U64Type.U_64_NAME;
import static co.llective.presto.hyena.types.U64Type.U_64_TYPE;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.function.OperatorType.DIVIDE;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.MODULUS;
import static com.facebook.presto.spi.function.OperatorType.MULTIPLY;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.SUBTRACT;
import static java.lang.Math.toIntExact;

public class U64IntOperators
{
    /**
     * Class holding methods for interacting between {@link U64Type} class and Integer (default int) ones.
     * Needed for human-friendly possibility to specify filters in SQL (e.g. u64_col < some_number).
     */
    private U64IntOperators() {}

    /**
     * Casts {@link U64Type} to integer (SQL Integer).
     *
     * @param value U64 number (SQL unsigned_long) to integer (SQL Integer)
     * @return integer value
     */
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castToInteger(@SqlType(U_64_NAME) long value)
    {
        try {
            return toIntExact(value);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for integer: " + value, e);
        }
    }

    /**
     * Compares {@link U64Type} value to integer (SQL Integer).
     * <p>
     * If long value is negative then result is always false.
     *
     * @param left u64 number
     * @param right integer number
     * @return Returns info if u64 value is equal to specified integer.
     */
    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equalToInteger(@SqlType(U_64_NAME) long left, @SqlType(StandardTypes.INTEGER) long right)
    {
        return U_64_TYPE.compareToSignedLong(left, right) == 0;
    }

    /**
     * Compares {@link U64Type} value to integer (SQL Integer).
     * <p>
     * If long value is negative then result is always false.
     *
     * @param left u64 number
     * @param right integer number
     * @return Returns info if u64 value is not equal to specified integer.
     */
    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqualToInteger(@SqlType(U_64_NAME) long left, @SqlType(StandardTypes.INTEGER) long right)
    {
        return U_64_TYPE.compareToSignedLong(left, right) != 0;
    }

    /**
     * Compares {@link U64Type} value to integer (SQL Integer).
     * <p>
     * If integer value is negative then result is always false.
     *
     * @param left u64 number
     * @param right integer number
     * @return Returns info if u64 value is less than specified integer.
     */
    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanInteger(@SqlType(U_64_NAME) long left, @SqlType(StandardTypes.INTEGER) long right)
    {
        return U_64_TYPE.compareToSignedLong(left, right) < 0;
    }

    /**
     * Compares {@link U64Type} value to integer (SQL Integer).
     * <p>
     * If long value is negative then result is always false.
     *
     * @param left u64 number
     * @param right integer number
     * @return Returns info if u64 value is less or equal than specified integer.
     */
    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqualInteger(@SqlType(U_64_NAME) long left, @SqlType(StandardTypes.INTEGER) long right)
    {
        return U_64_TYPE.compareToSignedLong(left, right) <= 0;
    }

    /**
     * Compares {@link U64Type} value to integer (SQL Integer).
     * <p>
     * If long value is negative then result is always true.
     *
     * @param left u64 number
     * @param right integer number
     * @return Returns info if u64 value is greater than specified integer.
     */
    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanInteger(@SqlType(U_64_NAME) long left, @SqlType(StandardTypes.INTEGER) long right)
    {
        return U_64_TYPE.compareToSignedLong(left, right) > 0;
    }

    /**
     * Compares {@link U64Type} value to integer (SQL Integer).
     * <p>
     * If long value is negative then result is always true.
     *
     * @param left u64 number
     * @param right integer number
     * @return Returns info if u64 value is greater or equal than specified integer.
     */
    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqualInteger(@SqlType(U_64_NAME) long left, @SqlType(StandardTypes.INTEGER) long right)
    {
        return U_64_TYPE.compareToSignedLong(left, right) >= 0;
    }

    /**
     * Handles between operator between {@link U64Type} and two Integers.
     *
     * @param value u64 number
     * @param min integer number
     * @param max integer number
     * @return Returns info if u64 is in between specified integers
     */
    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean betweenIntegers(@SqlType(U_64_NAME) long value, @SqlType(StandardTypes.INTEGER) long min, @SqlType(StandardTypes.INTEGER) long max)
    {
        return greaterThanOrEqualInteger(value, min)
                && lessThanOrEqualInteger(value, max);
    }

    @ScalarOperator(ADD)
    @SqlType(U_64_NAME)
    public static long add(@SqlType(U_64_NAME) long u64, @SqlType(StandardTypes.INTEGER) long integer)
    {
        try {
            return U_64_TYPE.addSignedInt(u64, (int) integer);
        }
        catch (ArithmeticException exc) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, exc.getMessage(), exc);
        }
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(U_64_NAME)
    public static long subtract(@SqlType(U_64_NAME) long u64, @SqlType(StandardTypes.INTEGER) long integer)
    {
        try {
            return U_64_TYPE.subtractSignedInt(u64, (int) integer);
        }
        catch (ArithmeticException exc) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, exc.getMessage(), exc);
        }
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(U_64_NAME)
    public static long multiply(@SqlType(U_64_NAME) long u64, @SqlType(StandardTypes.INTEGER) long integer)
    {
        try {
            return U_64_TYPE.multiplyBySignedInt(u64, (int) integer);
        }
        catch (ArithmeticException exc) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, exc.getMessage(), exc);
        }
    }

    @ScalarOperator(DIVIDE)
    @SqlType(U_64_NAME)
    public static long divide(@SqlType(U_64_NAME) long u64, @SqlType(StandardTypes.INTEGER) long integer)
    {
        try {
            return U_64_TYPE.divideBySignedInt(u64, (int) integer);
        }
        catch (ArithmeticException exc) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, exc.getMessage(), exc);
        }
    }

    @ScalarOperator(MODULUS)
    @SqlType(U_64_NAME)
    public static long modulus(@SqlType(U_64_NAME) long u64, @SqlType(StandardTypes.INTEGER) long integer)
    {
        try {
            return U_64_TYPE.moduloSignedInt(u64, (int) integer);
        }
        catch (ArithmeticException exc) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, exc.getMessage(), exc);
        }
    }
}
