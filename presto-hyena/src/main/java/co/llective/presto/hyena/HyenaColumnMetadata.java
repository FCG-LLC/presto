package co.llective.presto.hyena;

import co.llective.hyena.api.BlockType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;

public class HyenaColumnMetadata
        extends ColumnMetadata
{
    private BlockType blockType;

    public HyenaColumnMetadata(String name, Type type)
    {
        super(name, type);
    }

    public HyenaColumnMetadata(String name, Type type, BlockType blockType) {
        this(name, type);
        this.blockType = blockType;
    }

    public BlockType getBlockType()
    {
        return blockType;
    }
}
