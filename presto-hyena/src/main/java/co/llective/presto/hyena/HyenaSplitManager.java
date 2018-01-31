/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package co.llective.presto.hyena;

import co.llective.hyena.api.PartitionInfo;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import org.apache.commons.lang3.tuple.Pair;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class HyenaSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(HyenaSplitManager.class);

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

    public List<Pair<Long, Long>> extractAllowedTimestampRanges(TupleDomain.ColumnDomain<HyenaColumnHandle> tsDomain)
    {
        return tsDomain.getDomain().getValues().getValuesProcessor().transform(
                ranges -> {
                    ImmutableSet.Builder<Object> columnValues = ImmutableSet.builder();
                    List<Pair<Long, Long>> tsRanges = new ArrayList<>();

                    for (Range range : ranges.getOrderedRanges()) {
                        if (range.isSingleValue()) {
                            tsRanges.add(Pair.of((Long) range.getSingleValue(), (Long) range.getSingleValue()));
                        }
                        else {
                            Long low = null;
                            Long high = null;

                            if (range.getHigh().getValueBlock().isPresent()) {
                                high = (Long) range.getHigh().getValue();
                            }
                            if (range.getLow().getValueBlock().isPresent()) {
                                low = (Long) range.getLow().getValue();
                            }

                            if (low != null || high != null) {
                                tsRanges.add(Pair.of(low, high));
                            }
                        }
                    }

                    return tsRanges;
                },
                discreteValues -> {
                    if (discreteValues.isWhiteList()) {
                        List<Pair<Long, Long>> tsRanges = new ArrayList<>();
                        for (Object v : discreteValues.getValues()) {
                            tsRanges.add(Pair.of((Long) v, (Long) v));
                        }
                        return tsRanges;
                    }
                    return ImmutableList.of();
                },
                allOrNone -> ImmutableList.of());
    }

    public boolean prunePartitionOnTs(PartitionInfo partitionInfo, List<Pair<Long, Long>> allowedTsRanges)
    {
        if (allowedTsRanges.isEmpty()) {
            // Allow anything it ts included in filters
            return false;
        }

        for (Pair<Long, Long> range : allowedTsRanges) {
            Long practicalLeft = range.getLeft() == null ? 0L : range.getLeft();
            Long practicalRight = range.getRight() == null ? Long.MAX_VALUE : range.getRight();

            if (partitionInfo.getMinTs() <= practicalRight && partitionInfo.getMaxTs() >= practicalLeft) {
                return false; // cannot prune, the range overlaps
            }
        }

        // Nothing matched
        log.info("Pruned partition %d with TS range %d-%d", partitionInfo.getId(), partitionInfo.getMinTs(), partitionInfo.getMaxTs());
        return true;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        HyenaTableLayoutHandle layoutHandle = (HyenaTableLayoutHandle) layout;
        HyenaTableHandle tableHandle = layoutHandle.getTable();

        TupleDomain<HyenaColumnHandle> effectivePredicate = layoutHandle.getConstraint()
                .transform(HyenaColumnHandle.class::cast);

        List<PartitionInfo> partitions = hyenaSession.getAvailablePartitions();

        // TODO: this works for single node only as of now
        // TODO: do partition pruning basing on other features (sources?)

        List<Pair<Long, Long>> tsRanges = new ArrayList<>();
        effectivePredicate.getColumnDomains().map(columnDomains -> {
            for (TupleDomain.ColumnDomain<HyenaColumnHandle> domain : columnDomains) {
                if (HyenaMetadata.TIMESTAMP_COLUMN_NAME.equals(domain.getColumn().getColumnName())) {
                    tsRanges.addAll(extractAllowedTimestampRanges(domain));
                }
            }
            return true;
        });

        Node currentNode = nodeManager.getCurrentNode();
//        List<ConnectorSplit> splits = partitions.stream()
//                .filter(partitionInfo -> !prunePartitionOnTs(partitionInfo, tsRanges))
//                .map(partition -> new HyenaSplit(currentNode.getHostAndPort(), Sets.newHashSet(partition.getId()), effectivePredicate))
//                .collect(Collectors.toList());

        List<ConnectorSplit> splits = Arrays.asList(new HyenaSplit(currentNode.getHostAndPort(), new HashSet<>(), effectivePredicate));

        return new FixedSplitSource(splits);
    }
}
