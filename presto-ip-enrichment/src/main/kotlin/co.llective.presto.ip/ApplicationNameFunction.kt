package co.llective.presto.ip

import com.facebook.presto.spi.function.ScalarFunction
import com.facebook.presto.spi.function.SqlNullable
import com.facebook.presto.spi.function.SqlType
import com.facebook.presto.spi.type.StandardTypes
import cs.drill.ipfun.appname.ApplicationNameResolver
import io.airlift.slice.Slice
import io.airlift.slice.Slices
import org.slf4j.Logger
import org.apache.commons.lang3.StringUtils

object ApplicationNameFunction {

    @ScalarFunction("application_name")
    @SqlType(StandardTypes.VARCHAR)
    @JvmStatic
    fun applicationName(
            @SqlNullable @SqlType(StandardTypes.BIGINT) ip1: java.lang.Long,
            @SqlNullable @SqlType(StandardTypes.BIGINT) ip2: java.lang.Long,
            @SqlNullable @SqlType(StandardTypes.INTEGER) port: java.lang.Long
    ): Slice? {
        StringUtils.EMPTY
        Logger.ROOT_LOGGER_NAME
        val applicationName = ApplicationNameResolver.getApplicationName(ip1.toLong(), ip2.toLong(), port.toInt())
        return if (applicationName != null) Slices.utf8Slice(applicationName) else null
    }

    @ScalarFunction("application_name")
    @SqlType(StandardTypes.VARCHAR)
    @JvmStatic
    fun applicationName(
            @SqlNullable @SqlType(StandardTypes.BIGINT) ip1: java.lang.Long,
            @SqlNullable @SqlType(StandardTypes.BIGINT) ip2: java.lang.Long
    ): Slice? {
        val applicationName = ApplicationNameResolver.getApplicationName(ip1.toLong(), ip2.toLong())
        return Slices.utf8Slice(applicationName)
    }

}
