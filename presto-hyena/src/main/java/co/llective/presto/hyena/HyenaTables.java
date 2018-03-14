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
import co.llective.hyena.api.Column;
import co.llective.presto.hyena.types.U64Type;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static co.llective.presto.hyena.HyenaMetadata.PRESTO_HYENA_SCHEMA;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;

public class HyenaTables
{
    private final Map<SchemaTableName, List<ColumnMetadata>> tableColumns;
    private final SchemaTableName schemaTableName;
    private final HyenaTableHandle tableHandle;

    private final HyenaSession hyenaSession;

    public static final String PRESTO_HYENA_TABLE_NAME = "cs";

    @Inject
    public HyenaTables(HyenaSession hyenaSession)
    {
        this.hyenaSession = hyenaSession;

        schemaTableName = getSchemaTableName();

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumnsBuilder = ImmutableMap.builder();
        tableColumnsBuilder.put(schemaTableName, tableColumns(hyenaSession.getAvailableColumns()));
        tableColumns = tableColumnsBuilder.build();

        tableHandle = new HyenaTableHandle(schemaTableName);
    }

    private static SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(PRESTO_HYENA_SCHEMA, PRESTO_HYENA_TABLE_NAME);
    }

    private Type convertBlockType(BlockType blockType)
    {
        switch (blockType) {
            case I8Sparse:
            case I8Dense:
            case I16Sparse:
            case I16Dense:
            case I32Sparse:
            case I32Dense:
            case U8Sparse:
            case U8Dense:
            case U16Sparse:
            case U16Dense:
                return INTEGER;
            case U32Dense:
            case U32Sparse:
            case I64Dense:
            case I64Sparse:
                return BIGINT;
            case U64Dense:
            case U64Sparse:
                return U64Type.U_64_TYPE;
            case String:
                return VARCHAR;
            default:
                throw new RuntimeException("I don't know how to handle " + blockType.toString());
        }
    }

    private ColumnMetadata convertColumnMetadata(Column col)
    {
        return new ColumnMetadata(col.getName(), convertBlockType(col.getDataType()));
    }

    private List<ColumnMetadata> tableColumns(List<Column> hyenaColumns)
    {
        return hyenaColumns.stream().map(hyenaCol -> convertColumnMetadata(hyenaCol)).collect(Collectors.toList());
    }

    public HyenaTableHandle getTable(SchemaTableName tableName)
    {
        return tableHandle;
    }

    public List<SchemaTableName> getTables()
    {
        return Arrays.asList(schemaTableName);
    }

    public List<ColumnMetadata> getColumns(HyenaTableHandle tableHandle)
    {
        checkArgument(tableColumns.containsKey(tableHandle.getSchemaTableName()), "Table %s not registered", tableHandle.getSchemaTableName());
        return tableColumns.get(tableHandle.getSchemaTableName());
    }
}
