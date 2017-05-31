package co.llective.presto.hyena;

import co.llective.presto.hyena.api.HyenaApi;
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
    private final HyenaSession hyenaSession;

    @Inject
    public HyenaSplitManager(
            NodeManager nodeManager,
            HyenaSession session)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.hyenaSession = requireNonNull(session, "hyenaSession is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        HyenaTableLayoutHandle layoutHandle = (HyenaTableLayoutHandle) layout;
        HyenaTableHandle tableHandle = layoutHandle.getTable();

        TupleDomain<HyenaColumnHandle> effectivePredicate = layoutHandle.getConstraint()
                .transform(HyenaColumnHandle.class::cast);

        List<HyenaApi.PartitionInfo> partitions = hyenaSession.getAvailablePartitions();

        // TODO: this works for single node only as of now
        // TODO: do partition pruning basing on timestamp
        Node currentNode = nodeManager.getCurrentNode();
        List<ConnectorSplit> splits = partitions.stream()
                .map(partition -> new HyenaSplit(currentNode.getHostAndPort(), partition.id, effectivePredicate))
                .collect(Collectors.toList());
//        List<ConnectorSplit> splits = nodeManager.getAllNodes().stream()
//                .map(node -> new HyenaSplit(node.getHostAndPort(), tableHandle.getSchemaTableName(), effectivePredicate))
//                .collect(Collectors.toList());

        return new FixedSplitSource(splits);
    }
}