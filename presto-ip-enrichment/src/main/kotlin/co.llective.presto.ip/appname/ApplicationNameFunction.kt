package co.llective.presto.ip.appname

import com.facebook.presto.spi.function.ScalarFunction
import com.facebook.presto.spi.function.SqlNullable
import com.facebook.presto.spi.function.SqlType
import com.facebook.presto.spi.type.StandardTypes
import io.airlift.slice.Slice
import io.airlift.slice.Slices

object ApplicationNameFunction {

    @ScalarFunction("application_name")
    @SqlType(StandardTypes.VARCHAR)
    @JvmStatic
    fun applicationName(
            @SqlNullable @SqlType(StandardTypes.BIGINT) ip1: java.lang.Long,
            @SqlNullable @SqlType(StandardTypes.BIGINT) ip2: java.lang.Long,
            @SqlNullable @SqlType(StandardTypes.INTEGER) port: java.lang.Long
    ): Slice? {
        val applicationName = ApplicationNameResolver.getApplicationName(ip1.toLong(), ip2.toLong(), port.toInt())
        return applicationName?.let { Slices.utf8Slice(applicationName) }
    }

    @ScalarFunction("application_name")
    @SqlType(StandardTypes.VARCHAR)
    @JvmStatic
    fun applicationName(
            @SqlNullable @SqlType(StandardTypes.BIGINT) ip1: java.lang.Long,
            @SqlNullable @SqlType(StandardTypes.BIGINT) ip2: java.lang.Long
    ): Slice? {
        val applicationName = ApplicationNameResolver.getApplicationName(ip1.toLong(), ip2.toLong())
        return applicationName?.let { Slices.utf8Slice(applicationName) }
    }

}
