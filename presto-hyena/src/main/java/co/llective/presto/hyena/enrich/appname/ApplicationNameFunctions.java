package co.llective.presto.hyena.enrich.appname;

import co.llective.presto.hyena.types.U64Type;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class ApplicationNameFunctions
{
    private static final String U_64 = U64Type.U_64_NAME;
    private ApplicationNameFunctions() {}

    private static ApplicationNameCache appNameCache = ApplicationNameCache.getInstance();

    @ScalarFunction("application_name")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice ApplicationNameWithPort(
            @SqlType(U_64) long ip1,
            @SqlType(U_64) long ip2,
            @SqlType(StandardTypes.INTEGER) long port)
    {
        String appName = appNameCache.getApplicationName(ip1, ip2, port);
        return appName == null ? null : Slices.utf8Slice(appName);
    }

    @ScalarFunction("application_name")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice ApplicationName(
            @SqlType(U_64) long ip1,
            @SqlType(U_64) long ip2)
    {
        String appName = appNameCache.getApplicationName(ip1, ip2);
        return appName == null ? null : Slices.utf8Slice(appName);
    }
}
