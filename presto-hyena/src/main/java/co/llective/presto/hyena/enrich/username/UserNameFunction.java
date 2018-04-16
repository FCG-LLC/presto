package co.llective.presto.hyena.enrich.username;

import co.llective.presto.hyena.types.U64Type;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

@ScalarFunction("user_name")
@Description("Returns user name")
public class UserNameFunction
{
    private static final String U_64 = U64Type.U_64_NAME;
    private UserNameFunction() {}

    private static UserNameCache cache = UserNameCache.getInstance();

    @SqlType(StandardTypes.VARCHAR)
    public static Slice userName(
            @SqlType(U_64) long ip1,
            @SqlType(U_64) long ip2,
            @SqlType(U_64) long timestamp)
    {
        String userName = cache.getUserName(ip1, ip2, timestamp);
        return userName == null ? Slices.EMPTY_SLICE : Slices.utf8Slice(cache.getUserName(ip1, ip2, timestamp));
    }
}
