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

import co.llective.hyena.api.FilterType;
import co.llective.hyena.api.ScanAndFilters;
import co.llective.hyena.api.ScanComparison;
import co.llective.hyena.api.ScanFilter;
import co.llective.hyena.api.ScanOrFilters;
import co.llective.presto.hyena.types.U64Type;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.Ranges;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.primitives.UnsignedLong;
import io.airlift.slice.Slice;
import org.apache.commons.lang3.NotImplementedException;

import java.util.Map;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class HyenaPredicatesUtil
{
    public HyenaPredicatesUtil()
    {}

    public ScanOrFilters predicateToFilters(TupleDomain<HyenaColumnHandle> predicate)
    {
        ScanOrFilters filters = new ScanOrFilters();
        //no predicates, no filters, no problem
        if (!predicate.getDomains().isPresent()) {
            return filters;
        }

        Map<HyenaColumnHandle, Domain> domainMap = predicate.getDomains().get();

        domainMap.forEach((column, values) -> values.getValues().getValuesProcessor().consume(
                ranges -> {
                    filters.addAll(toOrScanFilters(column, ranges));
                },
                discreteValues -> {
                    throw new NotImplementedException("Discrete values are not handled yet"); },
                allOrNone -> { /* noop */ }));

        return filters;
    }

    private ScanOrFilters toOrScanFilters(HyenaColumnHandle column, Ranges ranges)
    {
        ScanOrFilters filters = new ScanOrFilters();
        for (Range range : ranges.getOrderedRanges()) {
            // handle = situation
            if (range.isSingleValue()) {
                filters.add(createSingleFilter(column, ScanComparison.Eq, range.getSingleValue()));
            }
            else {
                Marker high = range.getHigh();
                Marker low = range.getLow();

                if (low.getValueBlock().isPresent() && high.getValueBlock().isPresent()) {
                    // handle BETWEEN case
                    ScanAndFilters scanAndFilters = new ScanAndFilters();

                    scanAndFilters.add(createLowScanFilter(column, low));
                    scanAndFilters.add(createHighScanFilter(column, high));

                    filters.add(scanAndFilters);
                }
                else if (low.getValueBlock().isPresent()) {
                    // handle > or >= situation
                    ScanAndFilters filter = new ScanAndFilters(createLowScanFilter(column, low));
                    filters.add(filter);
                }
                else if (high.getValueBlock().isPresent()) {
                    // handle < or <= situation
                    ScanAndFilters filter = new ScanAndFilters(createHighScanFilter(column, high));
                    filters.add(filter);
                }
            }
        }
        return filters;
    }

    private ScanFilter createHighScanFilter(HyenaColumnHandle column, Marker high)
    {
        ScanComparison op = getRightBoundOp(high);
        Object value;

        if (column.getColumnType() == VARCHAR) {
            value = ((Slice) high.getValue()).toStringUtf8();
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
        return getNewScanFilter(column, op, value);
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
        Object value;

        if (column.getColumnType() == VARCHAR) {
            value = ((Slice) low.getValue()).toStringUtf8();
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
        return getNewScanFilter(column, op, value);
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

    private ScanAndFilters createSingleFilter(HyenaColumnHandle column, ScanComparison comparisonOperator, Object singleValue)
    {
        ScanAndFilters andFilters = new ScanAndFilters();
        if (column.getColumnType() == VARCHAR) {
            andFilters.add(getNewScanFilter(column, ScanComparison.Contains, singleValue));
        }
        else {
            Long val = (Long) singleValue;
            andFilters.add(getNewScanFilter(column, comparisonOperator, val));
        }
        return andFilters;
    }

    private ScanFilter getNewScanFilter(HyenaColumnHandle column, ScanComparison op, Object value)
    {
        if (column.getHyenaType().mapToFilterType() == FilterType.String) {
            Slice slice = (Slice) value;
            return createStringFilter(column, op, slice.toStringUtf8());
        }
        return new ScanFilter(
            column.getOrdinalPosition(),
            op,
            column.getHyenaType().mapToFilterType(),
            value);
    }

    private ScanFilter createStringFilter(HyenaColumnHandle column, ScanComparison op, String value)
    {
        if (value.startsWith("%") && value.endsWith("%")) {
            op = ScanComparison.Contains;
        }
        else if (value.startsWith("%")) {
            op = ScanComparison.EndsWith;
        }
        else if (value.endsWith("%")) {
            op = ScanComparison.StartsWith;
        }

        return new ScanFilter(
                column.getOrdinalPosition(),
                op,
                column.getHyenaType().mapToFilterType(),
                escapeLikeChars(value));
    }

    private String escapeLikeChars(String likedValue)
    {
        String escapedString = likedValue;
        if (escapedString.length() == 0) {
            return escapedString;
        }
        if (escapedString.startsWith("%")) {
            escapedString = escapedString.substring(1, escapedString.length());
        }
        if (escapedString.endsWith("%")) {
            escapedString = escapedString.substring(0, escapedString.length() - 1);
        }
        return escapedString;
    }
}
