package co.llective.presto.hyena.enrich.topdisco;

import co.llective.presto.hyena.types.U64Type;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

@ScalarFunction("ip_name")
public class IpNameFunction
{
    private static final String U_64 = U64Type.U_64_NAME;
    private IpNameFunction() {}

    private static TopdiscoProvider topdiscoProvider = TopdiscoProvider.getInstance();

    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice ipName(
            @SqlType(U_64) long ip1,
            @SqlType(U_64) long ip2)
    {
        String name = topdiscoProvider.getIpName(ip1, ip2);
        return name == null ? null : Slices.utf8Slice(name);
    }
}
