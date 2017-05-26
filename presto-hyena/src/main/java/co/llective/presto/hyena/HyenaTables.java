package co.llective.presto.hyena;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;
import java.util.*;

import static co.llective.presto.hyena.HyenaMetadata.PRESTO_HYENA_SCHEMA;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;

public class HyenaTables
{
    private final Map<SchemaTableName, List<ColumnMetadata>> tableColumns;
    private final SchemaTableName schemaTableName;
    private final HyenaTableHandle tableHandle;


    @Inject
    public HyenaTables(HyenaConfig config)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumnsBuilder = ImmutableMap.builder();

        schemaTableName = PseudoTable.getSchemaTableName();
        tableHandle = new HyenaTableHandle(schemaTableName);
        tableColumnsBuilder.put(schemaTableName, PseudoTable.getColumns());
        tableColumns = tableColumnsBuilder.build();

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

    public static class PseudoTable
    {
        private static final List<ColumnMetadata> COLUMNS = ImmutableList.of(
                new ColumnMetadata("timestamp", BIGINT),
                new ColumnMetadata("source", INTEGER),
                new ColumnMetadata("col_01", BIGINT),
                new ColumnMetadata("col_02", BIGINT),
                new ColumnMetadata("col_03", BIGINT),
                new ColumnMetadata("col_04", BIGINT),
                new ColumnMetadata("col_05", BIGINT),
                new ColumnMetadata("col_06", BIGINT),
                new ColumnMetadata("col_07", BIGINT),
                new ColumnMetadata("col_08", BIGINT),
                new ColumnMetadata("col_09", BIGINT),
                new ColumnMetadata("col_10", BIGINT),
                new ColumnMetadata("col_11", BIGINT),
                new ColumnMetadata("col_12", BIGINT),
                new ColumnMetadata("col_13", BIGINT),
                new ColumnMetadata("col_14", BIGINT),
                new ColumnMetadata("col_15", BIGINT),
                new ColumnMetadata("col_16", BIGINT),
                new ColumnMetadata("col_17", BIGINT));


        private static final String TABLE_NAME = "cs";

        public static List<ColumnMetadata> getColumns()
        {
            return COLUMNS;
        }

        public static SchemaTableName getSchemaTableName()
        {
            return new SchemaTableName(PRESTO_HYENA_SCHEMA, TABLE_NAME);
        }

        public static OptionalInt getTimestampColumn()
        {
            return OptionalInt.of(0);
        }

        public static OptionalInt getServerAddressColumn()
        {
            return OptionalInt.of(-1);
        }
    }
}