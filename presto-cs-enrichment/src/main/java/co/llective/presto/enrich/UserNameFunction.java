package co.llective.presto.enrich;

import co.llective.presto.hyena.types.U64Type;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

@ScalarFunction("user_name")
@Description("Returns user name")
public class UserNameFunction
{
    private UserNameFunction() {}

    @SqlType(StandardTypes.VARCHAR)
    public static Slice userName(
            @SqlNullable @SqlType(U64Type.U_64_NAME) Long ip1,
            @SqlNullable @SqlType(U64Type.U_64_NAME) Long ip2,
            @SqlType(U64Type.U_64_NAME) long timestamp)
    {
        return Slices.utf8Slice("ip-timestamp - " + ip1 + ":" + ip2 + ":" + timestamp);
    }
}
