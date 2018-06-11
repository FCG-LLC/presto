package com.facebook.presto.spi.predicate;

import io.airlift.slice.Slice;

import java.util.Objects;

public class LikeValue
{
    public LikeValue(Slice pattern) {
        this(pattern, false);
    }

    public LikeValue(Slice pattern, boolean negated) {
        this.pattern = pattern;
        this.negated = negated;
    }

    Slice pattern;
    boolean negated;

    public Slice getPattern()
    {
        return pattern;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        LikeValue likeValue = (LikeValue) o;
        return negated == likeValue.negated &&
                Objects.equals(pattern, likeValue.pattern);
    }

    @Override
    public int hashCode()
    {

        return Objects.hash(pattern, negated);
    }
}
