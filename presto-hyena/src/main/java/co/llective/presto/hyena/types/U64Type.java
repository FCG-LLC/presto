package co.llective.presto.hyena.types;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.AbstractFixedWidthType;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.primitives.UnsignedLong;

public final class U64Type
        extends AbstractFixedWidthType
{
    public static final U64Type U_64_TYPE = new U64Type();
    public static final String U_64_NAME = "unsigned_long";

    private U64Type()
    {
        super(TypeSignature.parseTypeSignature(U_64_NAME), long.class, 8);
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return compareTo(leftBlock, leftPosition, rightBlock, rightPosition) == 0;
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long leftValue = getLong(leftBlock, leftPosition);
        long rightValue = getLong(rightBlock, rightPosition);
        return compareUnsignedLongs(leftValue, rightValue);
    }

    public int compareUnsignedLongs(long leftValue, long rightValue)
    {
        return UnsignedLong.fromLongBits(leftValue).compareTo(UnsignedLong.fromLongBits(rightValue));
    }

    @Override
    public long hash(Block block, int position)
    {
        return block.hash(position, 0, getFixedSize());
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return getLong(block, position);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            writeLong(blockBuilder, block.getLong(position, 0));
        }
    }

    @Override
    public long getLong(Block block, int position)
    {
        return block.getLong(position, 0);
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        blockBuilder
                .writeLong(value)
                .closeEntry();
    }

}
