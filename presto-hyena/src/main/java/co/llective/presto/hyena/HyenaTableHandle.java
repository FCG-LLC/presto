package co.llective.presto.hyena;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HyenaTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;

    @JsonCreator
    public HyenaTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HyenaTableHandle that = (HyenaTableHandle) o;
        return Objects.equals(schemaTableName, that.schemaTableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaTableName", schemaTableName)
                .toString();
    }
}
