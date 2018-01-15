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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class InternalAggregationFunction
{
    private final String name;
    private final List<Type> parameterTypes;
    private final Type intermediateType;
    private final Type finalType;
    private final boolean decomposable;
    private final boolean orderSensitive;
    private final AccumulatorFactoryBinder factory;

    public InternalAggregationFunction(String name, List<Type> parameterTypes, Type intermediateType, Type finalType, boolean decomposable, boolean orderSensitive, AccumulatorFactoryBinder factory)
    {
        this.name = requireNonNull(name, "name is null");
        checkArgument(!name.isEmpty(), "name is empty");
        this.parameterTypes = ImmutableList.copyOf(requireNonNull(parameterTypes, "parameterTypes is null"));
        this.intermediateType = requireNonNull(intermediateType, "intermediateType is null");
        this.finalType = requireNonNull(finalType, "finalType is null");
        this.decomposable = decomposable;
        this.orderSensitive = orderSensitive;
        this.factory = requireNonNull(factory, "factory is null");
    }

    public String name()
    {
        return name;
    }

    public List<Type> getParameterTypes()
    {
        return parameterTypes;
    }

    public Type getFinalType()
    {
        return finalType;
    }

    public Type getIntermediateType()
    {
        return intermediateType;
    }

    /**
     * Indicates that the aggregation can be decomposed, and run as partial aggregations followed by a final aggregation to combine the intermediate results
     */
    public boolean isDecomposable()
    {
        return decomposable;
    }

    /**
     * Indicates that the aggregation is sensitive to input order
     */
    public boolean isOrderSensitive()
    {
        return orderSensitive;
    }

    public AccumulatorFactory bind(List<Integer> inputChannels, Optional<Integer> maskChannel)
    {
        return factory.bind(inputChannels, maskChannel, ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), null);
    }

    public AccumulatorFactory bind(List<Integer> inputChannels, Optional<Integer> maskChannel, List<Type> sourceTypes, List<Integer> orderByChannels, List<SortOrder> orderings, PagesIndex.Factory pagesIndexFactory)
    {
        return factory.bind(inputChannels, maskChannel, sourceTypes, orderByChannels, orderings, pagesIndexFactory);
    }

    @VisibleForTesting
    public AccumulatorFactoryBinder getAccumulatorFactoryBinder()
    {
        return factory;
    }
}
