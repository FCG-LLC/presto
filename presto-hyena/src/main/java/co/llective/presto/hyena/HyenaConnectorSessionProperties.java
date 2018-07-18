package co.llective.presto.hyena;

import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

public class HyenaConnectorSessionProperties
{
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public HyenaConnectorSessionProperties(HyenaConfig hyenaConfig)
    {
        sessionProperties = ImmutableList.of();
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
