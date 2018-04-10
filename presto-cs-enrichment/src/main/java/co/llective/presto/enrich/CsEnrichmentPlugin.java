package co.llective.presto.enrich;

import com.facebook.presto.spi.Plugin;

import java.util.Collections;
import java.util.Set;

public class CsEnrichmentPlugin
        implements Plugin
{
    @Override
    public Set<Class<?>> getFunctions() {
        return Collections.emptySet();
    }
}
