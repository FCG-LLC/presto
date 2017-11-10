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

import co.llective.presto.hyena.api.HyenaApi;
import co.llective.presto.hyena.type.IpAddressType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
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
    public HyenaTables(HyenaConfig config)
    {
        hyenaSession = new NativeHyenaSession(config);

        schemaTableName = getSchemaTableName();

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumnsBuilder = ImmutableMap.builder();

        List<HyenaApi.Column> availableColumns = Arrays.asList(new HyenaApi.Column(HyenaApi.BlockType.Byte128Dense, "ip"));
        tableColumnsBuilder.put(schemaTableName, tableColumns(availableColumns));
//        tableColumnsBuilder.put(schemaTableName, tableColumns(hyenaSession.getAvailableColumns()));
        tableColumns = tableColumnsBuilder.build();

        tableHandle = new HyenaTableHandle(schemaTableName);
    }

    private static SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(PRESTO_HYENA_SCHEMA, PRESTO_HYENA_TABLE_NAME);
    }

    private Type convertBlockType(HyenaApi.BlockType blockType)
    {
        switch (blockType) {
            case Int8Sparse:
            case Int16Sparse:
            case Int32Sparse:
                return INTEGER;
            case Int64Dense:
            case Int64Sparse:
                return BIGINT;
            case String:
                return VARCHAR;
            case Byte128Dense:
                return IpAddressType.IPADDRESS;
//                return VarbinaryType.VARBINARY;
            default:
                throw new RuntimeException("I don't know how to handle " + blockType.toString());
        }
    }

    private ColumnMetadata convertColumnMetadata(HyenaApi.Column col)
    {
        return new ColumnMetadata(col.name, convertBlockType(col.dataType));
    }

    private List<ColumnMetadata> tableColumns(List<HyenaApi.Column> hyenaColumns)
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
