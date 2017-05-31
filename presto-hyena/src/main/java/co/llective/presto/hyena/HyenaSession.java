package co.llective.presto.hyena;

import co.llective.presto.hyena.api.HyenaApi;

import java.util.List;

public interface HyenaSession {
    List<HyenaApi.PartitionInfo> getAvailablePartitions();
    HyenaApi.ScanResult scan(HyenaApi.ScanRequest req);
}
