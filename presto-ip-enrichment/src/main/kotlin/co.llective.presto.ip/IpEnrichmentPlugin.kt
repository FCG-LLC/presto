package co.llective.presto.ip

import com.facebook.presto.spi.Plugin

import java.util.Collections.emptySet

class IpEnrichmentPlugin : Plugin {
    override fun getFunctions(): Set<Class<*>> {
        return emptySet<Class<*>>()
    }
}
