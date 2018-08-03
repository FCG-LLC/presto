package co.llective.presto.hyena;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static co.llective.presto.hyena.HyenaConfig.STREAMING_ENABLED;
import static co.llective.presto.hyena.HyenaConfig.STREAMING_ENABLED_DESC;
import static co.llective.presto.hyena.HyenaConfig.STREAMING_RECORDS_LIMIT;
import static co.llective.presto.hyena.HyenaConfig.STREAMING_RECORDS_LIMIT_DESC;
import static co.llective.presto.hyena.HyenaConfig.STREAMING_RECORDS_THRESHOLD;
import static co.llective.presto.hyena.HyenaConfig.STREAMING_RECORDS_THRESHOLD_DESC;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longSessionProperty;

public class HyenaConnectorSessionProperties
{
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public HyenaConnectorSessionProperties(HyenaConfig hyenaConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanSessionProperty(
                        STREAMING_ENABLED,
                        STREAMING_ENABLED_DESC,
                        hyenaConfig.getStreamingEnabled(),
                        false),
                longSessionProperty(
                        STREAMING_RECORDS_LIMIT,
                        STREAMING_RECORDS_LIMIT_DESC,
                        hyenaConfig.getStreamingRecordsLimit(),
                        false),
                longSessionProperty(
                        STREAMING_RECORDS_THRESHOLD,
                        STREAMING_RECORDS_THRESHOLD_DESC,
                        hyenaConfig.getStreamingRecordsThreshold(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean getStreamingEnabled(ConnectorSession session)
    {
        return session.getProperty(STREAMING_ENABLED, Boolean.class);
    }

    public static long getStreamingRecordsLimit(ConnectorSession session)
    {
        return session.getProperty(STREAMING_RECORDS_LIMIT, Long.class);
    }

    public static long getStreamingRecordsThreshold(ConnectorSession session)
    {
        return session.getProperty(STREAMING_RECORDS_THRESHOLD, Long.class);
    }
}
