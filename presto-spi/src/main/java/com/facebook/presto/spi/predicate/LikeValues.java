package com.facebook.presto.spi.predicate;

import java.util.List;

public interface LikeValues
{
    int getLikeCount();

    List<LikeValue> getLikeValues();
}
