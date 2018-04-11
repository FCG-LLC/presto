package co.llective.presto.enrich;

import com.facebook.presto.spi.Plugin;

import java.util.HashSet;
import java.util.Set;

public class CsEnrichmentPlugin
        implements Plugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        Set<Class<?>> functions = new HashSet<>();
        functions.add(UserNameFunction.class);
        return functions;
    }
}
