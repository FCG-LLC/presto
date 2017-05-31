package co.llective.presto.hyena;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HyenaSplit
        implements ConnectorSplit
{
    private final HostAddress address;
    private final Long partitionId;
    private final TupleDomain<HyenaColumnHandle> effectivePredicate;

    @JsonCreator
    public HyenaSplit(
            @JsonProperty("address") HostAddress address,
            @JsonProperty("partitionId") Long partitionId,
            @JsonProperty("effectivePredicate") TupleDomain<HyenaColumnHandle> effectivePredicate)
    {
        this.address = requireNonNull(address, "address is null");
        this.partitionId = requireNonNull(partitionId, "partitionId is null");
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
    }

    @JsonProperty
    public HostAddress getAddress()
    {
        return address;
    }

    @JsonProperty
    public Long getPartitionId() { return partitionId; }

    @JsonProperty
    public TupleDomain<HyenaColumnHandle> getEffectivePredicate()
    {
        return effectivePredicate;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of(address);
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("address", address)
                .add("partitionId", partitionId)
                .toString();
    }
}
