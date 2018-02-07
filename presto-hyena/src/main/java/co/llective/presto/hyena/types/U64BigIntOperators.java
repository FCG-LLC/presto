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

import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

import static co.llective.presto.hyena.types.U64Type.U_64_NAME;
import static co.llective.presto.hyena.types.U64Type.U_64_TYPE;
import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;

/**
 * Class holding methods for interacting between {@link U64Type} class and BigInt ones.
 * Needed for human-friendly possibility to specify filters in SQL (e.g. u64_col < some_number).
 */
public class U64BigIntOperators
{
    private U64BigIntOperators() {}

    /**
     * Casts {@link U64Type} to long (SQL BigInt).
     *
     * @param value U64 number (SQL unsigned_long) to long (SQL BigInteger)
     * @return
     */
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToBigInt(@SqlType(U_64_NAME) long value)
    {
        return value;
    }

    /**
     * Compares {@link U64Type} value to long (SQL BigInt).
     * <p>
     * If long value is negative then result is always false.
     * Keeping that in mind now it's only possible to compare to max 2^63.
     *
     * @param left u64 number
     * @param right long number
     * @return Returns info if u64 value is less than specified long.
     */
    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanBigInt(@SqlType(U_64_NAME) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        if (right < 0) {
            return false;
        }
        else {
            return U_64_TYPE.compareUnsignedLongs(left, right) < 0;
        }
    }

    /**
     * Compares {@link U64Type} value to long (SQL BigInt).
     * <p>
     * If long value is negative then result is always false.
     * Keeping that in mind now it's only possible to compare to max 2^63.
     *
     * @param left u64 number
     * @param right long number
     * @return Returns info if u64 value is less or equal than specified long.
     */
    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqualBigInt(@SqlType(U_64_NAME) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        if (right < 0) {
            return false;
        }
        else {
            return U_64_TYPE.compareUnsignedLongs(left, right) <= 0;
        }
    }

    /**
     * Compares {@link U64Type} value to long (SQL BigInt).
     * <p>
     * If long value is negative then result is always true.
     * Keeping that in mind now it's only possible to compare to max 2^63.
     *
     * @param left u64 number
     * @param right long number
     * @return Returns info if u64 value is greater than specified long.
     */
    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanBigInt(@SqlType(U_64_NAME) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        if (right < 0) {
            return true;
        }
        else {
            return U_64_TYPE.compareUnsignedLongs(left, right) > 0;
        }
    }

    /**
     * Handles between operator between {@link U64Type} and two BigInts.
     *
     * @param value u64 number
     * @param min long number
     * @param max long number
     * @return Returns info if u64 is in between specified longs
     */
    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean betweenBigInts(@SqlType(U_64_NAME) long value, @SqlType(StandardTypes.BIGINT) long min, @SqlType(StandardTypes.BIGINT) long max)
    {
        return greaterThanOrEqualBigInt(value, min)
                && lessThanOrEqualBigInt(value, max);
    }

    /**
     * Compares {@link U64Type} value to long (SQL BigInt).
     * <p>
     * If long value is negative then result is always true.
     * Keeping that in mind now it's only possible to compare to max 2^63.
     *
     * @param left u64 number
     * @param right long number
     * @return Returns info if u64 value is greater or equal than specified long.
     */
    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqualBigInt(@SqlType(U_64_NAME) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        if (right < 0) {
            return true;
        }
        else {
            return U_64_TYPE.compareUnsignedLongs(left, right) >= 0;
        }
    }
}
