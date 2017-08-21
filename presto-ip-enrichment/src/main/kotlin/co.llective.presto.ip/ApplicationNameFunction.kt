package co.llective.presto.ip

import com.facebook.presto.spi.function.Description
import com.facebook.presto.spi.function.ScalarFunction
import com.facebook.presto.spi.function.SqlNullable
import com.facebook.presto.spi.function.SqlType
import com.facebook.presto.spi.type.StandardTypes
import io.airlift.slice.Slice
import io.airlift.slice.Slices

@ScalarFunction("application_name")
@Description("Returns application name")
object ApplicationNameFunction {

    @SqlType(StandardTypes.VARCHAR)
    @JvmStatic
    fun applicationNameWithPort(
            @SqlNullable @SqlType(StandardTypes.BIGINT) ip1: java.lang.Long,
            @SqlNullable @SqlType(StandardTypes.BIGINT) ip2: java.lang.Long,
            @SqlNullable @SqlType(StandardTypes.INTEGER) port: java.lang.Integer
    ): Slice {
        return Slices.utf8Slice("app_name_with_port")
    }

    @SqlType(StandardTypes.VARCHAR)
    @JvmStatic
    fun applicationName(
            @SqlNullable @SqlType(StandardTypes.BIGINT) ip1: java.lang.Long,
            @SqlNullable @SqlType(StandardTypes.BIGINT) ip2: java.lang.Long
    ): Slice {
        return Slices.utf8Slice("app_name")
    }

}
