package co.llective.presto.hyena;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import javax.inject.Singleton;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class HyenaClientModule
        implements Module {
    private final String connectorId;

    public HyenaClientModule(String connectorId) {
        this.connectorId = connectorId;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(HyenaConnector.class).in(Scopes.SINGLETON);
        binder.bind(HyenaMetadata.class).in(Scopes.SINGLETON);
        binder.bind(HyenaSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(HyenaRecordSetProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(HyenaConfig.class);
    }

    @Singleton
    @Provides
    public static HyenaSession createHyenaSession(
            HyenaConfig config) {
        return new NativeHyenaSession(config);
    }
}
