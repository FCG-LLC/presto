package co.llective.presto.hyena;

import co.llective.presto.hyena.api.HyenaApi;

import java.io.IOException;
import java.util.List;

public class NativeHyenaSession implements HyenaSession {
    private final HyenaApi hyenaApi;

    public NativeHyenaSession() {
        hyenaApi = new HyenaApi();
    }

    @Override
    public List<HyenaApi.PartitionInfo> getAvailablePartitions() {
        // TODO: catalog caching
        try {
            return hyenaApi.refreshCatalog().availablePartitions;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public HyenaApi.ScanResult scan(HyenaApi.ScanRequest req) {
        try {
            return hyenaApi.scan(req);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
}
