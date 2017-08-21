package co.llective.presto.ip

import com.facebook.presto.spi.Plugin
import com.google.common.collect.ImmutableSet

class IpEnrichmentPlugin : Plugin {
    override fun getFunctions(): Set<Class<*>> {
        return ImmutableSet.of(ApplicationNameFunction::class.java)
    }
}
