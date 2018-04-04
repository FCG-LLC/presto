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

import co.llective.hyena.api.Catalog;
import co.llective.hyena.api.FilterType;
import co.llective.hyena.api.ScanComparison;
import co.llective.hyena.api.ScanFilter;
import co.llective.hyena.api.ScanFilterBuilder;
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
    private Catalog catalog;

    public HyenaPredicatesUtil(Catalog catalog)
    {
        this.catalog = catalog;
    }

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
                    filters.addAll(toScanFilters(catalog, column, ranges));
                },
                discreteValues -> {
                    throw new NotImplementedException("Discrete values are not handled yet"); },
                allOrNone -> { /* noop */ }));

        return filters;
    }

    private List<ScanFilter> toScanFilters(Catalog catalog, HyenaColumnHandle column, Ranges ranges)
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
                    ScanFilter filter = createLowScanFilter(catalog, column, low);
                    filters.add(filter);
                }

                // handle < or <= situation
                if (high.getValueBlock().isPresent()) {
                    ScanFilter filter = createHighScanFilter(catalog, column, high);
                    filters.add(filter);
                }
            }
        }
        return filters;
    }

    private ScanFilter createHighScanFilter(Catalog catalog, HyenaColumnHandle column, Marker high)
    {
        ScanFilterBuilder filterBuilder = new ScanFilterBuilder(catalog);
        filterBuilder = filterBuilder.withColumn(column.getOrdinalPosition());

        ScanComparison op = getRightBoundOp(high);
        filterBuilder = filterBuilder.withOp(op);

        if (column.getColumnType() == VARCHAR) {
            filterBuilder = filterBuilder.withStringValue(((Slice) high.getValue()).toStringUtf8());
        }
        else if (column.getColumnType() == U64Type.U_64_TYPE) {
            //TODO: replace with proper handling of U64 filters
            Long value = (Long) high.getValue();
            if (value < 0) {
                filterBuilder = filterBuilder.withOp(ScanComparison.Gt);
                filterBuilder = filterBuilder.withValue(UnsignedLong.MAX_VALUE.longValue());
            }
            else {
                filterBuilder = filterBuilder.withValue(value);
            }
        }
        else {
            filterBuilder = filterBuilder.withValue((Long) high.getValue());
        }
        return filterBuilder.build();
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

    private ScanFilter createLowScanFilter(Catalog catalog, HyenaColumnHandle column, Marker low)
    {
        ScanFilterBuilder filterBuilder = new ScanFilterBuilder(catalog);
        filterBuilder = filterBuilder.withColumn(column.getOrdinalPosition());

        ScanComparison op = getLeftBoundOp(low);
        filterBuilder = filterBuilder.withOp(op);

        if (column.getColumnType() == VARCHAR) {
            filterBuilder = filterBuilder.withStringValue(((Slice) low.getValue()).toStringUtf8());
        }
        else if (column.getColumnType() == U64Type.U_64_TYPE) {
            //TODO: replace with proper handling of U64 filters
            Long value = (Long) low.getValue();
            if (value < 0) {
                filterBuilder = filterBuilder.withOp(ScanComparison.GtEq);
                filterBuilder = filterBuilder.withValue(UnsignedLong.ZERO.longValue());
            }
            else {
                filterBuilder = filterBuilder.withValue(value);
            }
        }
        else {
            filterBuilder = filterBuilder.withValue((Long) low.getValue());
        }
        return filterBuilder.build();
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
            return new ScanFilter(column.getOrdinalPosition(), comparisonOperator, FilterType.String, val, Optional.of(val.toStringUtf8()));
        }
        else {
            Long val = (Long) singleValue;
            return new ScanFilter(column.getOrdinalPosition(), comparisonOperator, column.getHyenaType().mapToFilterType(), val, Optional.of(""));
        }
    }
}
