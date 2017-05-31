package co.llective.presto.hyena;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class HyenaConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "hyena";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new HyenaHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    binder -> binder.bind(NodeManager.class).toInstance(context.getNodeManager()),
                    new HyenaModule(connectorId),
                    new HyenaClientModule(connectorId));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(HyenaConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}

