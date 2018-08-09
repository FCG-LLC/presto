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

import co.llective.presto.hyena.util.TimeBoundaries;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.primitives.UnsignedLong;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    private static final int CHUNKS_NO = 5;

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        HyenaTableLayoutHandle layoutHandle = (HyenaTableLayoutHandle) layout;

        TupleDomain<HyenaColumnHandle> effectivePredicate = layoutHandle.getConstraint()
                .transform(HyenaColumnHandle.class::cast);

        Node currentNode = nodeManager.getCurrentNode();

        HyenaPredicatesUtil predicatesUtil = new HyenaPredicatesUtil();
        Optional<TimeBoundaries> timeRange = predicatesUtil.getTsConstraints(effectivePredicate);
        List<ConnectorSplit> splits;
        if (timeRange.isPresent()) {
            List<TimeBoundaries> splitBoundaries = splitTimeBoundaries(timeRange.get());
            splits = splitBoundaries.stream()
                        .map(timeBoundaries -> new HyenaSplit(currentNode.getHostAndPort(), effectivePredicate, Optional.of(timeBoundaries)))
                        .collect(Collectors.toList());
        } else {
            splits = Collections.singletonList(new HyenaSplit(currentNode.getHostAndPort(), effectivePredicate, Optional.empty()));
        }

        return new FixedSplitSource(splits);
    }

    private List<TimeBoundaries> splitTimeBoundaries(TimeBoundaries timeRange)
    {
        Long min = timeRange.getStart();
        Long max = timeRange.getEnd();

        List<TimeBoundaries> splitBoundaries = new ArrayList<>();

        if (min == null) {
            // -inf
            //TODO: change to min value of record in DB
            min = 1531138692090000L;
            splitBoundaries.add(TimeBoundaries.of(0L, min));
        }
        if (max == null) {
            max = (System.currentTimeMillis() + TimeUnit.MINUTES.toNanos(10)) * 1000;
            splitBoundaries.add(TimeBoundaries.of(max, UnsignedLong.MAX_VALUE.longValue()));
            // +inf
        }

        long offset = (max - min) / CHUNKS_NO;

        for (int i = 0; i < CHUNKS_NO - 1; i++) {
            TimeBoundaries splitTimeBoundaries = TimeBoundaries.of(min + i * offset, min + (i + 1) * offset);
            splitBoundaries.add(splitTimeBoundaries);
        }
        // in case of some modulo left
        splitBoundaries.add(TimeBoundaries.of(min + (CHUNKS_NO - 1) * offset, max));
        return splitBoundaries;
    }
}
