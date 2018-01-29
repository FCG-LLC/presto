package co.llective.presto.hyena.types;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.AbstractLongType;
import com.facebook.presto.spi.type.StandardTypes;

import static co.llective.presto.hyena.types.U64Type.U_64_NAME;
import static co.llective.presto.hyena.types.U64Type.U_64_TYPE;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static java.lang.Math.toIntExact;

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
}
