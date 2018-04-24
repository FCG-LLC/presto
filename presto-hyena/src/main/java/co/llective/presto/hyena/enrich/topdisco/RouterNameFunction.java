package co.llective.presto.hyena.enrich.topdisco;

import co.llective.presto.hyena.types.U64Type;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

@ScalarFunction("router_name")
public class RouterNameFunction
{
    private static final String U_64 = U64Type.U_64_NAME;
    private RouterNameFunction() {}

    private static TopdiscoProvider topdiscoProvider = TopdiscoProvider.getInstance();

    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice routerName(
            @SqlType(U_64) long ip1,
            @SqlType(U_64) long ip2)
    {
        String routerName = topdiscoProvider.getRouterName(ip1, ip2);
        return routerName == null ? null : Slices.utf8Slice(routerName);
    }
}
