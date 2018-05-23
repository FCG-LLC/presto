package com.facebook.presto.spi.predicate;

import io.airlift.slice.Slice;

public interface LikeValue
{
    Slice getLikeValue();
}
