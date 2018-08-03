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
import co.llective.presto.hyena.util.TimeBoundaries;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;
import io.airlift.slice.Slice;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class HyenaPredicatesUtil
{
    public HyenaPredicatesUtil()
    {}

    public Optional<TimeBoundaries> getTsConstraints(TupleDomain<HyenaColumnHandle> predicate)
    {
        if (predicate.getColumnDomains().isPresent()) {
            List<TupleDomain.ColumnDomain<HyenaColumnHandle>> columnDomains = predicate.getColumnDomains().get();
            Optional<Domain> tsDomain = columnDomains.stream().filter(x -> x.getColumn().getColumnName().equals("timestamp")).map(TupleDomain.ColumnDomain::getDomain).findFirst();
            if (tsDomain.isPresent()) {
                Long lowestValue = Long.MAX_VALUE;
                Long highestValue = Long.MIN_VALUE;
                Domain domain = tsDomain.get();
                if (domain.isSingleValue()) {
                    Long ts = (Long) domain.getSingleValue();
                    lowestValue = ts;
                    highestValue = ts;
                }
                else {
                    for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                        Marker high = range.getHigh();
                        Marker low = range.getLow();

                        if (low.getValueBlock().isPresent()) {
                            // handle > or >= situation
                            Long lowValue = (Long) low.getValue();
                            if (lowValue < lowestValue) {
                                lowestValue = lowValue;
                            }
                        }
                        if (high.getValueBlock().isPresent()) {
                            // handle < or <= situation
                            Long maxValue = (Long) high.getValue();
                            if (maxValue > highestValue) {
                                highestValue = maxValue;
                            }
                        }
                    }
                }
                if (lowestValue == Long.MAX_VALUE) {
                    lowestValue = UnsignedLong.ZERO.longValue();
                }
                if (highestValue == Long.MIN_VALUE) {
                    highestValue = UnsignedLong.MAX_VALUE.longValue();
                }
                return Optional.of(TimeBoundaries.of(lowestValue, highestValue));
            }
        }
        return Optional.empty();
    }

    /**
     * Method that transforms given {@link TupleDomain} into {@link ScanOrFilters}
     * Example:
     * input:
     * col1 - range11, range12
     * col2 - range21, range22
     *
     * output:
     * (range11 AND range21) OR
     * (range11 AND range22) OR
     * (range12 AND range21) OR
     * (range12 AND range22)
     *
     * @param predicate {@link TupleDomain}
     * @return {@link ScanOrFilters}
     */
    public ScanOrFilters predicateToFilters(TupleDomain<HyenaColumnHandle> predicate)
    {
        ScanOrFilters orFilters = new ScanOrFilters();

        if (predicate.getDomains().isPresent()) {
            Map<HyenaColumnHandle, Domain> domainMap = predicate.getDomains().get();

            if (domainMap.isEmpty()) {
                return orFilters;
            }

            List<List<ColumnRangePair>> allRanges = domainMap.entrySet().stream()
                    .map(entry -> {
                        HyenaColumnHandle column = entry.getKey();
                        List<Range> ranges = entry.getValue().getValues().getRanges().getOrderedRanges();
                        List<ColumnRangePair> columnRangePairs = ranges.stream()
                                .map(range -> new ColumnRangePair(column, range))
                                .collect(Collectors.toList());
                        return columnRangePairs;
                    })
                    .collect(Collectors.toList());

            List<List<ColumnRangePair>> mixedRanges = Lists.cartesianProduct(allRanges);

            for (List<ColumnRangePair> andRanges : mixedRanges) {
                ScanAndFilters andFilters = new ScanAndFilters();

                for (ColumnRangePair columnRange : andRanges) {
                    // create filter and add to ANDs
                    HyenaColumnHandle column = columnRange.getColumn();
                    Range range = columnRange.getRange();

                    andFilters.addAll(scanAndFiltersFromRange(column, range));
                }

                // add ANDs to OR
                orFilters.add(andFilters);
            }
        }
        return orFilters;
    }

    private ScanAndFilters scanAndFiltersFromRange(HyenaColumnHandle column, Range range)
    {
        ScanAndFilters andFilters = new ScanAndFilters();

        // handle = situation
        if (range.isSingleValue()) {
            //TODO: split string%string case
            ScanFilter singleFilter = createSingleFilter(column, ScanComparison.Eq, range.getSingleValue());
            andFilters.add(singleFilter);
        }
        else {
            Marker high = range.getHigh();
            Marker low = range.getLow();

            if (low.getValueBlock().isPresent() && high.getValueBlock().isPresent()) {
                // handle BETWEEN case
                andFilters.add(createLowScanFilter(column, low));
                andFilters.add(createHighScanFilter(column, high));
            }
            else if (low.getValueBlock().isPresent()) {
                // handle > or >= situation
                ScanFilter greaterFilter = createLowScanFilter(column, low);
                andFilters.add(greaterFilter);
            }
            else if (high.getValueBlock().isPresent()) {
                // handle < or <= situation
                ScanFilter lessFilter = createHighScanFilter(column, high);
                andFilters.add(lessFilter);
            }
        }

        return andFilters;
    }

    private class ColumnRangePair
    {
        private HyenaColumnHandle column;
        private Range range;

        public HyenaColumnHandle getColumn()
        {
            return column;
        }

        public Range getRange()
        {
            return range;
        }

        ColumnRangePair(HyenaColumnHandle column, Range range)
        {
            this.column = column;
            this.range = range;
        }

        @Override
        public String toString()
        {
            return "ColumnRangePair{" +
                    "column=" + column +
                    ", range=(" + range.getLow().getValue() + ", " + range.getHigh().getValue() +
                    ")}";
        }
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

    private ScanFilter createSingleFilter(HyenaColumnHandle column, ScanComparison comparisonOperator, Object singleValue)
    {
        if (column.getColumnType() == VARCHAR) {
            return getNewScanFilter(column, ScanComparison.Contains, singleValue);
        }
        else {
            Long val = (Long) singleValue;
            return getNewScanFilter(column, comparisonOperator, val);
        }
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

    /**
     * It checks if on the beginning of the string there is a special character/separator (0x11, 0x12, 0x13 - LikeHackUtility#StringMetaCharacter)
     * to determine what kind of like filter value this string is.
     *
     * If there is no special character in string it means that it is equal/matches operator.
     * @param filterValue input string
     * @return pair, where left side is string without special character and on the right side is {@link ScanComparison} operator.
     */
    private Pair<String, ScanComparison> extractStringOperator(String filterValue)
    {
        if (filterValue.isEmpty()) {
            return Pair.of(filterValue, ScanComparison.Matches);
        }

        // Special characters are defined in LikeHackUtility#StringMetaCharacter
        switch (filterValue.charAt(0)) {
            case 0x11:
                return Pair.of(filterValue.substring(1), ScanComparison.StartsWith);
            case 0x12:
                return Pair.of(filterValue.substring(1), ScanComparison.EndsWith);
            case 0x13:
                return Pair.of(filterValue.substring(1), ScanComparison.Contains);
            default:
                return Pair.of(filterValue, ScanComparison.Matches);
        }
    }

    private ScanFilter createStringFilter(HyenaColumnHandle column, ScanComparison op, String value)
    {
        Pair<String, ScanComparison> filterWithOperator = extractStringOperator(value);
        return new ScanFilter(
                column.getOrdinalPosition(),
                filterWithOperator.getRight(),
                column.getHyenaType().mapToFilterType(),
                filterWithOperator.getLeft());
    }
}
