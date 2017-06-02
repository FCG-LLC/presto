package co.llective.presto.hyena;

import co.llective.presto.hyena.api.HyenaApi;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.List;

public class NativeHyenaSession implements HyenaSession {
    private final HyenaApi hyenaApi;

    public NativeHyenaSession() {
        hyenaApi = new HyenaApi();
    }

    public void close() {
        hyenaApi.close();
    }

    @Override
    public NativeHyenaSession recordSetProviderSession() {
        return new NativeHyenaSession();
    }

    private HyenaApi.Catalog refreshCatalog() {
        // TODO: catalog caching
        try {
            return hyenaApi.refreshCatalog();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public List<HyenaApi.Column> getAvailableColumns() {
        return refreshCatalog().columns;
    }

    @Override
    public List<HyenaApi.PartitionInfo> getAvailablePartitions() {
        return refreshCatalog().availablePartitions;
    }

    @Override
    public HyenaApi.ScanResult scan(HyenaApi.ScanRequest req, HyenaApi.HyenaOpMetadata metadata) {
        try {
            return hyenaApi.scan(req, metadata);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
}
