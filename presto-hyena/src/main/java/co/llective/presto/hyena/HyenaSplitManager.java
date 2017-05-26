package co.llective.presto.hyena;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;

import javax.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class HyenaSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;

    @Inject
    public HyenaSplitManager(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        HyenaTableLayoutHandle layoutHandle = (HyenaTableLayoutHandle) layout;
        HyenaTableHandle tableHandle = layoutHandle.getTable();

        TupleDomain<HyenaColumnHandle> effectivePredicate = layoutHandle.getConstraint()
                .transform(HyenaColumnHandle.class::cast);

        List<ConnectorSplit> splits = nodeManager.getAllNodes().stream()
                .map(node -> new HyenaSplit(node.getHostAndPort(), tableHandle.getSchemaTableName(), effectivePredicate))
                .collect(Collectors.toList());

        return new FixedSplitSource(splits);
    }
}