package co.llective.presto.hyena;

import co.llective.presto.hyena.api.HyenaApi;

import java.util.List;

public interface HyenaSession {
    // We need to create new session since Nanomsg connections are not thread safe...
    NativeHyenaSession recordSetProviderSession();

    List<HyenaApi.PartitionInfo> getAvailablePartitions();
    List<HyenaApi.Column> getAvailableColumns();
    HyenaApi.ScanResult scan(HyenaApi.ScanRequest req, HyenaApi.HyenaOpMetadata metadataOrNull);

    void close();
}
