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

import co.llective.hyena.api.BlockType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HyenaColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type columnType;
    private final BlockType hyenaType;
    private final long ordinalPosition;

    @JsonCreator
    public HyenaColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("hyenaType") BlockType hyenaType,
            @JsonProperty("ordinalPosition") int ordinalPosition)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.hyenaType = hyenaType;
        this.ordinalPosition = ordinalPosition;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public BlockType getHyenaType() {
        return hyenaType;
    }

    @JsonProperty
    public long getOrdinalPosition()
    {
        return ordinalPosition;
    }

    public ColumnMetadata toColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType);
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
        HyenaColumnHandle that = (HyenaColumnHandle) o;
        return Objects.equals(columnName, that.columnName) &&
                Objects.equals(columnType, that.columnType) &&
                Objects.equals(hyenaType, that.hyenaType) &&
                Objects.equals(ordinalPosition, that.ordinalPosition);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, columnType, ordinalPosition);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .add("hyenaType", hyenaType)
                .add("ordinalPosition", ordinalPosition)
                .toString();
    }
}
