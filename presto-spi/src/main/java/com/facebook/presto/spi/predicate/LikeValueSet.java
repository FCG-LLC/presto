package com.facebook.presto.spi.predicate;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;

import java.util.function.Consumer;
import java.util.function.Function;

public class LikeValueSet
        implements ValueSet
{
    private static final Type type = VarcharType.VARCHAR;
    private final Slice value;

    public LikeValueSet(Slice value) {
        this.value = value;
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public boolean isNone()
    {
        return false;
    }

    @Override
    public boolean isAll()
    {
        return value.toStringUtf8().equals("%");
    }

    @Override
    public boolean isSingleValue()
    {
        return value.length() != 0;
    }

    @Override
    public Object getSingleValue()
    {
        return value;
    }

    @Override
    public boolean containsValue(Object value)
    {
        return isSingleValue();
    }

    @Override
    public LikeValue getLikeValue() {
        return () -> value;
    }

    @Override
    public ValuesProcessor getValuesProcessor()
    {
        return new ValuesProcessor() {
            @Override
            public <T> T transform(Function<Ranges, T> rangesFunction, Function<DiscreteValues, T> discreteValuesFunction, Function<AllOrNone, T> allOrNoneFunction, Function<LikeValue, T> likeValueFunction)
            {
                return likeValueFunction.apply(getLikeValue());
            }

            @Override
            public void consume(Consumer<Ranges> rangesConsumer, Consumer<DiscreteValues> discreteValuesConsumer, Consumer<AllOrNone> allOrNoneConsumer, Consumer<LikeValue> likeValueConsumer)
            {
                likeValueConsumer.accept(getLikeValue());
            }
        };
    }

    @Override
    public ValueSet intersect(ValueSet other)
    {
        if (!other.getType().equals(getType())) {
            throw new IllegalStateException(String.format("Mismatched types: %s vs %s", getType(), other.getType()));
        }
        if (!(other instanceof LikeValueSet)) {
            throw new IllegalStateException(String.format("ValueSet is not a SortedRangeSet: %s", other.getClass()));
        }
//        LikeValueSet otherLike = (LikeValueSet) other;
        //TODO: Add algorithm of merging likes


        return this;
    }

    @Override
    public ValueSet union(ValueSet other)
    {
        //TODO: Add algorithm of adding likes
        return this;
    }

    @Override
    public ValueSet complement()
    {
        //TODO: Add algorithm of complement set
        return null;
    }

    @Override
    public String toString(ConnectorSession session)
    {
        return "[like " + value.toStringUtf8() + "]";
    }
}
