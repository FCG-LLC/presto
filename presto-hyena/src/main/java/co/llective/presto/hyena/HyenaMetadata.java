package co.llective.presto.hyena;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class HyenaMetadata
        implements ConnectorMetadata
{
    public static final String PRESTO_HYENA_SCHEMA = "hyena";
    private static final List<String> SCHEMA_NAMES = ImmutableList.of(PRESTO_HYENA_SCHEMA);

    private final HyenaTables localFileTables;

    @Inject
    public HyenaMetadata(HyenaTables localFileTables)
    {
        this.localFileTables = requireNonNull(localFileTables, "localFileTables is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return SCHEMA_NAMES;
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        return localFileTables.getTable(tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        HyenaTableHandle tableHandle = (HyenaTableHandle) table;
        return new ConnectorTableMetadata(tableHandle.getSchemaTableName(), localFileTables.getColumns(tableHandle));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return localFileTables.getTables();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        HyenaTableHandle tableHandle = (HyenaTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new HyenaTableLayoutHandle(tableHandle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        HyenaTableLayoutHandle layout = (HyenaTableLayoutHandle) handle;
        return new ConnectorTableLayout(layout);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table)
    {
        HyenaTableHandle tableHandle = (HyenaTableHandle) table;
        return getColumnHandles(tableHandle);
    }

    private Map<String, ColumnHandle> getColumnHandles(HyenaTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : localFileTables.getColumns(tableHandle)) {
            columnHandles.put(column.getName(), new HyenaColumnHandle(column.getName(), column.getType(), index));
            index++;
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((HyenaColumnHandle) columnHandle).toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            HyenaTableHandle tableHandle = localFileTables.getTable(tableName);
            if (tableHandle != null) {
                columns.put(tableName, localFileTables.getColumns(tableHandle));
            }
        }
        return columns.build();
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return emptyList();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return emptyMap();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }
}
