package co.llective.presto.hyena.enrich.ipstring;

import co.llective.presto.hyena.enrich.util.IpUtil;
import co.llective.presto.hyena.types.U64Type;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

@ScalarFunction("ip_to_str")
public class IpToStringFunction
{
    private static final String U_64 = U64Type.U_64_NAME;
    private IpToStringFunction() {}

    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice ipToString(
            @SqlType(U_64) long ip1,
            @SqlType(U_64) long ip2)
    {
        IpUtil.IpPair ip = new IpUtil.IpPair(ip1, ip2);
        return Slices.utf8Slice(ip.toString());
    }
}
