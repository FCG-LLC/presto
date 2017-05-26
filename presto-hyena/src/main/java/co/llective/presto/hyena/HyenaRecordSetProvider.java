package co.llective.presto.hyena;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class HyenaRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final HyenaTables localFileTables;

    @Inject
    public HyenaRecordSetProvider(HyenaTables localFileTables)
    {
        this.localFileTables = requireNonNull(localFileTables, "localFileTables is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        requireNonNull(split, "split is null");
        HyenaSplit localFileSplit = (HyenaSplit) split;

        ImmutableList.Builder<HyenaColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((HyenaColumnHandle) handle);
        }

        return new HyenaRecordSet(localFileTables, localFileSplit, handles.build());
    }
}

