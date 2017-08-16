package co.llective.presto.ip

import com.facebook.presto.spi.Plugin
import com.facebook.presto.spi.function.*
import com.facebook.presto.spi.type.StandardTypes
import com.google.common.collect.ImmutableSet
import io.airlift.slice.Slice
import io.airlift.slice.Slices

class IpEnrichmentPlugin : Plugin {
    override fun getFunctions(): Set<Class<*>> {
        return ImmutableSet.of(DummyFunction.Companion::class.java)//javaClass)//::class.java)
    }
}

class DummyFunction {
    companion object {
        @TypeParameter("T")
        @ScalarFunction("DUMMY")
        @Description("Returns dummy string")
        @SqlType(StandardTypes.VARCHAR)
//        @JvmStatic
        fun goForDummySlice(@SqlNullable @SqlType("T") value: Slice): Slice {
            return Slices.utf8Slice("dummy")
        }

        @TypeParameter("T")
        @ScalarFunction("DUMMY")
        @Description("Returns dummy string")
        @SqlType(StandardTypes.VARCHAR)
//        @JvmStatic
        fun goForDummyInt(@SqlNullable @SqlType("T") value: Integer): Slice {
            return Slices.utf8Slice("dummy")
        }

        @TypeParameter("T")
        @ScalarFunction("DUMMY")
        @Description("Returns dummy string")
        @SqlType(StandardTypes.VARCHAR)
//        @JvmStatic
        fun goForDummyLong(@SqlNullable @SqlType("T") value: Long): Slice {
            return Slices.utf8Slice("dummy")
        }
    }
}
