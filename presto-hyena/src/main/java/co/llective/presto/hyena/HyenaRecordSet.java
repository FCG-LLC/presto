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
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HyenaRecordSet
        implements RecordSet
{
    private final List<HyenaColumnHandle> columns;
    private final List<Type> columnTypes;
    private final TupleDomain<HyenaColumnHandle> effectivePredicate;
    private final Optional<TimeBoundaries> timeBoundaries;
    private final HyenaSession hyenaSession;
    private final ConnectorSession connectorSession;

    public HyenaRecordSet(HyenaSession hyenaSession, ConnectorSession connectorSession, HyenaSplit split, List<HyenaColumnHandle> columns)
    {
        this.columns = requireNonNull(columns, "column handles is null");
        requireNonNull(split, "split is null");

        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (HyenaColumnHandle column : columns) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        this.effectivePredicate = split.getEffectivePredicate();
        this.timeBoundaries = split.getTimeBoundaries();

        this.hyenaSession = requireNonNull(hyenaSession, "hyenaSession is null");
        this.connectorSession = requireNonNull(connectorSession, "ConnectorSession is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new HyenaRecordCursor(hyenaSession, connectorSession, columns, effectivePredicate, timeBoundaries);
    }
}
