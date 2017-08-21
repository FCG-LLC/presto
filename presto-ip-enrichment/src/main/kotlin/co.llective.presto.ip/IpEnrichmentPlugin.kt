package co.llective.presto.ip

import com.facebook.presto.spi.Plugin
import com.facebook.presto.spi.function.*
import com.facebook.presto.spi.type.StandardTypes
import com.google.common.collect.ImmutableSet
import io.airlift.slice.Slice
import io.airlift.slice.Slices

class IpEnrichmentPlugin : Plugin {
    override fun getFunctions(): Set<Class<*>> {
        return ImmutableSet.of(DummyFunction::class.java)
    }
}

@ScalarFunction("DUMMY")
@Description("Returns dummy string")
object DummyFunction {
        @SqlType(StandardTypes.VARCHAR)
        @JvmStatic
        fun goForDummySlice(@SqlNullable @SqlType(StandardTypes.VARCHAR) value: Slice): Slice {
            return Slices.utf8Slice("dummySlice")
        }
}
