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
import com.facebook.presto.spi.type.AbstractLongType;
import com.facebook.presto.spi.type.StandardTypes;

import static co.llective.presto.hyena.types.U64Type.U_64_NAME;
import static co.llective.presto.hyena.types.U64Type.U_64_TYPE;
import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;

public final class U64Operators
{
    private U64Operators() {}

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equal(@SqlType(U_64_NAME) long left, @SqlType(U_64_NAME) long right)
    {
        return U_64_TYPE.compareUnsignedLongs(left, right) == 0;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(@SqlType(U_64_NAME) long left, @SqlType(U_64_NAME) long right)
    {
        return U_64_TYPE.compareUnsignedLongs(left, right) != 0;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(U_64_NAME) long left, @SqlType(U_64_NAME) long right)
    {
        return U_64_TYPE.compareUnsignedLongs(left, right) < 0;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(U_64_NAME) long left, @SqlType(U_64_NAME) long right)
    {
        return U_64_TYPE.compareUnsignedLongs(left, right) <= 0;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(U_64_NAME) long left, @SqlType(U_64_NAME) long right)
    {
        return U_64_TYPE.compareUnsignedLongs(left, right) > 0;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(U_64_NAME) long left, @SqlType(U_64_NAME) long right)
    {
        return U_64_TYPE.compareUnsignedLongs(left, right) >= 0;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType(U_64_NAME) long value, @SqlType(U_64_NAME) long min, @SqlType(U_64_NAME) long max)
    {
        return U_64_TYPE.compareUnsignedLongs(min, value) <= 0
                && U_64_TYPE.compareUnsignedLongs(value, max) <= 0;
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(U_64_NAME) long value)
    {
        return AbstractLongType.hash(value);
    }
}
