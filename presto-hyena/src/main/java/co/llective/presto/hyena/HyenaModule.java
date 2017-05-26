package co.llective.presto.hyena;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class HyenaModule
        implements Module
{
    private final String connectorId;

    public HyenaModule(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(HyenaConfig.class);

        binder.bind(HyenaConnector.class).in(Scopes.SINGLETON);
        binder.bind(HyenaMetadata.class).in(Scopes.SINGLETON);
        binder.bind(HyenaSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(HyenaRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(HyenaHandleResolver.class).in(Scopes.SINGLETON);

        binder.bind(HyenaTables.class).in(Scopes.SINGLETON);
    }
}
