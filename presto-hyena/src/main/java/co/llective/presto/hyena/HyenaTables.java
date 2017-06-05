package co.llective.presto.hyena;

import co.llective.presto.hyena.api.HyenaApi;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

import static co.llective.presto.hyena.HyenaMetadata.PRESTO_HYENA_SCHEMA;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;

public class HyenaTables
{
    private final Map<SchemaTableName, List<ColumnMetadata>> tableColumns;
    private final SchemaTableName schemaTableName;
    private final HyenaTableHandle tableHandle;

    private final HyenaSession hyenaSession;

    public final static String PREST_HYENA_TABLE_NAME = "cs";

    @Inject
    public HyenaTables(HyenaConfig config)
    {
       hyenaSession = new NativeHyenaSession();

        schemaTableName = getSchemaTableName();

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumnsBuilder = ImmutableMap.builder();
        tableColumnsBuilder.put(schemaTableName, tableColumns(hyenaSession.getAvailableColumns()));
        tableColumns = tableColumnsBuilder.build();

        tableHandle = new HyenaTableHandle(schemaTableName);
    }

    private static SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(PRESTO_HYENA_SCHEMA, PREST_HYENA_TABLE_NAME);
    }

    private Type convertBlockType(HyenaApi.BlockType blockType) {
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
            default:
                throw new RuntimeException("I don't know how to handle "+blockType.toString());
        }
    }

    private ColumnMetadata convertColumnMetadata(HyenaApi.Column col) {
        return new ColumnMetadata(col.name, convertBlockType(col.data_type));
    }

    private List<ColumnMetadata> tableColumns(List<HyenaApi.Column> hyenaColumns) {
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

//
//    public static class PseudoTable
//    {
//        private static final List<ColumnMetadata> COLUMNS = ImmutableList.of(
//                new ColumnMetadata("timestamp", BIGINT),
//                new ColumnMetadata("source", INTEGER),
//                new ColumnMetadata("col_01", BIGINT),
//                new ColumnMetadata("col_02", BIGINT),
//                new ColumnMetadata("col_03", BIGINT),
//                new ColumnMetadata("col_04", BIGINT),
//                new ColumnMetadata("col_05", BIGINT),
//                new ColumnMetadata("col_06", BIGINT),
//                new ColumnMetadata("col_07", BIGINT),
//                new ColumnMetadata("col_08", BIGINT),
//                new ColumnMetadata("col_09", BIGINT),
//                new ColumnMetadata("col_10", BIGINT),
//                new ColumnMetadata("col_11", BIGINT),
//                new ColumnMetadata("col_12", BIGINT),
//                new ColumnMetadata("col_13", BIGINT),
//                new ColumnMetadata("col_14", BIGINT),
//                new ColumnMetadata("col_15", BIGINT),
//                new ColumnMetadata("col_16", BIGINT),
//                new ColumnMetadata("col_17", BIGINT));
//
//
//        private static final String TABLE_NAME = "cs";
//
//        public static List<ColumnMetadata> getColumns()
//        {
//            return COLUMNS;
//        }
//
//        public static SchemaTableName getSchemaTableName()
//        {
//            return new SchemaTableName(PRESTO_HYENA_SCHEMA, TABLE_NAME);
//        }
//
//        public static OptionalInt getTimestampColumn()
//        {
//            return OptionalInt.of(0);
//        }
//
//        public static OptionalInt getServerAddressColumn()
//        {
//            return OptionalInt.of(-1);
//        }
//    }
}