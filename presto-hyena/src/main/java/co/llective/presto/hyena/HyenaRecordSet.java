package co.llective.presto.hyena;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class HyenaRecordSet
        implements RecordSet
{
    private final List<HyenaColumnHandle> columns;
    private final List<Type> columnTypes;
    private final HostAddress address;
    private final TupleDomain<HyenaColumnHandle> effectivePredicate;
    private final SchemaTableName tableName;
    private final HyenaTables localFileTables;

    public HyenaRecordSet(HyenaTables localFileTables, HyenaSplit split, List<HyenaColumnHandle> columns)
    {
        this.columns = requireNonNull(columns, "column handles is null");
        requireNonNull(split, "split is null");

        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (HyenaColumnHandle column : columns) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        this.address = Iterables.getOnlyElement(split.getAddresses());
        this.effectivePredicate = split.getEffectivePredicate();
        this.tableName = split.getTableName();

        this.localFileTables = requireNonNull(localFileTables, "localFileTables is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new HyenaRecordCursor(localFileTables, columns, tableName, address, effectivePredicate);
    }
}

