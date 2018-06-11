package com.facebook.presto.spi.predicate;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LikeValueSet
        implements ValueSet
{
    private static final Type type = VarcharType.VARCHAR;
    private final List<LikeValue> likes;

    @JsonCreator
    public LikeValueSet(
            @JsonProperty("likes") List<LikeValue> values)
    {
        this.likes = new ArrayList<>(values);
    }

    static LikeValueSet none() {
        return new LikeValueSet(Collections.emptyList());
    }

    //TODO: all()?

    static LikeValueSet of(Type type, Object first, Object... rest) {
        //TODO: Add rest of ranges
        List<LikeValue> likeList = new ArrayList<>(rest.length + 1);
        likeList.add(new LikeValue((Slice) first));
        for (Object like : rest) {
            likeList.add(new LikeValue((Slice) like));
        }
        return copyOf(type, likeList);
    }

    static LikeValueSet of(Type type, LikeValue first, LikeValue... rest) {
        //TODO: Add rest of ranges
        List<LikeValue> likeList = new ArrayList<>(rest.length + 1);
        likeList.add(first);
        likeList.addAll(Arrays.asList(rest));
        return copyOf(type, likeList);
    }

    @JsonCreator
    public static LikeValueSet copyOf(
            @JsonProperty("type") Type type,
            @JsonProperty("likes") List<LikeValue> likes)
    {
        return new LikeValueSet(likes);
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
        //TODO:
        return getLikeValues().getLikeValues().stream().allMatch(x -> x.pattern.toStringUtf8().equals("%"));
    }

    @Override
    public boolean isSingleValue()
    {
        return likes.isEmpty();
    }

    @Override
    public Object getSingleValue()
    {
        return likes.get(0);
    }

    @Override
    public boolean containsValue(Object value)
    {
        return isSingleValue();
    }

    @Override
    public LikeValues getLikeValues() {
        return new LikeValues() {
            @Override
            public int getLikeCount()
            {
                return LikeValueSet.this.likes.size();
            }

            @Override
            public List<LikeValue> getLikeValues()
            {
                return LikeValueSet.this.likes;
            }
        };
    }

    @Override
    public ValuesProcessor getValuesProcessor()
    {
        return new ValuesProcessor() {
            @Override
            public <T> T transform(Function<Ranges, T> rangesFunction, Function<DiscreteValues, T> discreteValuesFunction, Function<AllOrNone, T> allOrNoneFunction, Function<LikeValues, T> likeValueFunction)
            {
                return likeValueFunction.apply(getLikeValues());
            }

            @Override
            public void consume(Consumer<Ranges> rangesConsumer, Consumer<DiscreteValues> discreteValuesConsumer, Consumer<AllOrNone> allOrNoneConsumer, Consumer<LikeValues> likeValueConsumer)
            {
                likeValueConsumer.accept(getLikeValues());
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
        //TODO: Add algorithm of intersecting likes


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
        return "[likes " + likes.stream().map(
                y -> y.pattern.toStringUtf8()).collect(Collectors.joining(", ")
        ) + "]";
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        LikeValueSet that = (LikeValueSet) o;
        return Objects.equals(likes, that.likes);
    }

    @Override
    public int hashCode()
    {

        return Objects.hash(likes);
    }
}
