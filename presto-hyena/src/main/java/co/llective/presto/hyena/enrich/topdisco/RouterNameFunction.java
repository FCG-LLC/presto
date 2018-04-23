package co.llective.presto.hyena.enrich.topdisco;

import co.llective.presto.hyena.types.U64Type;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

@ScalarFunction("rounter_name")
public class RouterNameFunction
{
    private static final String U_64 = U64Type.U_64_NAME;
    private RouterNameFunction() {}

    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice routerName(
            @SqlType(U_64) long ip1,
            @SqlType(U_64) long ip2)
    {
        return null;
    }
}
