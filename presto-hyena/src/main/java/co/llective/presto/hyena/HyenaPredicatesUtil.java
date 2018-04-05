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

import co.llective.hyena.api.ScanComparison;
import co.llective.hyena.api.ScanFilter;
import co.llective.presto.hyena.types.U64Type;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.Ranges;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.primitives.UnsignedLong;
import io.airlift.slice.Slice;
import org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class HyenaPredicatesUtil
{
    public HyenaPredicatesUtil()
    {}

    public List<ScanFilter> predicateToFilters(TupleDomain<HyenaColumnHandle> predicate)
    {
        List<ScanFilter> filters = new ArrayList<>();
        //no predicates, no filters, no problem
        if (!predicate.getDomains().isPresent()) {
            return filters;
        }

        Map<HyenaColumnHandle, Domain> domainMap = predicate.getDomains().get();

        domainMap.forEach((column, values) -> values.getValues().getValuesProcessor().consume(
                ranges -> {
                    filters.addAll(toScanFilters(column, ranges));
                },
                discreteValues -> {
                    throw new NotImplementedException("Discrete values are not handled yet"); },
                allOrNone -> { /* noop */ }));

        return filters;
    }

    private List<ScanFilter> toScanFilters(HyenaColumnHandle column, Ranges ranges)
    {
        List<ScanFilter> filters = new ArrayList<>();
        for (Range range : ranges.getOrderedRanges()) {
            // handle = situation
            if (range.isSingleValue()) {
                filters.add(createSingleFilter(column, ScanComparison.Eq, range.getSingleValue()));
            }
            else {
                Marker high = range.getHigh();
                Marker low = range.getLow();

                // handle > or >= situation
                if (low.getValueBlock().isPresent()) {
                    ScanFilter filter = createLowScanFilter(column, low);
                    filters.add(filter);
                }

                // handle < or <= situation
                if (high.getValueBlock().isPresent()) {
                    ScanFilter filter = createHighScanFilter(column, high);
                    filters.add(filter);
                }
            }
        }
        return filters;
    }

    private ScanFilter createHighScanFilter(HyenaColumnHandle column, Marker high)
    {
        ScanComparison op = getRightBoundOp(high);
        String stringValue = null;
        Object value;

        if (column.getColumnType() == VARCHAR) {
            value = (Slice) high.getValue();
            stringValue = ((Slice) high.getValue()).toStringUtf8();
        }
        else {
            value = (Long) high.getValue();
            if (column.getColumnType() == U64Type.U_64_TYPE) {
                //TODO: replace with proper handling of U64 filters
                Long longValue = (Long) high.getValue();
                if (longValue < 0) {
                    op = ScanComparison.Gt;
                    value = UnsignedLong.MAX_VALUE.longValue();
                }
            }
        }
        return getNewScanFilter(column, op, value, stringValue);
    }

    private ScanComparison getRightBoundOp(Marker high)
    {
        if (high.getBound() == Marker.Bound.BELOW) {
            return ScanComparison.Lt;
        }
        else if (high.getBound() == Marker.Bound.EXACTLY) {
            return ScanComparison.LtEq;
        }
        else {
            throw new UnsupportedOperationException("We don't know how to handle this yet - high values and neither below nor exactly marker present?");
        }
    }

    private ScanFilter createLowScanFilter(HyenaColumnHandle column, Marker low)
    {
        ScanComparison op = getLeftBoundOp(low);
        String stringValue = null;
        Object value;

        if (column.getColumnType() == VARCHAR) {
            value = (Slice) low.getValue();
            stringValue = ((Slice) low.getValue()).toStringUtf8();
        }
        else {
            value = (Long) low.getValue();
            //TODO: replace with proper handling of U64 filters
            if (column.getColumnType() == U64Type.U_64_TYPE) {
                Long longValue = (Long) low.getValue();
                if (longValue < 0) {
                    op = ScanComparison.GtEq;
                    value = UnsignedLong.ZERO.longValue();
                }
            }
        }
        return getNewScanFilter(column, op, value, stringValue);
    }

    private ScanComparison getLeftBoundOp(Marker low)
    {
        switch (low.getBound()) {
            case ABOVE:
                return ScanComparison.Gt;
            case EXACTLY:
                return ScanComparison.GtEq;
            default:
                throw new IllegalStateException("Not expected bound for left value filter: " + low.getBound());
        }
    }

    private ScanFilter createSingleFilter(HyenaColumnHandle column, ScanComparison comparisonOperator, Object singleValue)
    {
        if (column.getColumnType() == VARCHAR) {
            Slice val = (Slice) singleValue;
            return getNewScanFilter(column, comparisonOperator, val, val.toStringUtf8());
        }
        else {
            Long val = (Long) singleValue;
            return getNewScanFilter(column, comparisonOperator, val, null);
        }
    }

    private ScanFilter getNewScanFilter(HyenaColumnHandle column, ScanComparison op, Object value, String stringValue)
    {
        Optional<String> stringOptional = stringValue == null ? Optional.of("") : Optional.of(stringValue);
        return new ScanFilter(
            column.getOrdinalPosition(),
            op,
            column.getHyenaType().mapToFilterType(),
            value,
            stringOptional);
    }
}
