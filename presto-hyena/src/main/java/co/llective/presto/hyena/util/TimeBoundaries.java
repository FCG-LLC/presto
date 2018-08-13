package co.llective.presto.hyena.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TimeBoundaries
{
    private final Long start;
    private final Long end;

    @JsonCreator
    public TimeBoundaries(@JsonProperty("start") Long start, @JsonProperty("end") Long end)
    {
        this.start = start;
        this.end = end;
    }

    public static TimeBoundaries of(Long start, Long end)
    {
        return new TimeBoundaries(start, end);
    }

    @JsonProperty
    public Long getStart()
    {
        return start;
    }

    @JsonProperty
    public Long getEnd()
    {
        return end;
    }

    @Override
    public String toString()
    {
        return "TimeBoundaries{" +
                "start=" + start +
                ", end=" + end +
                '}';
    }
}
