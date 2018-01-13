package co.llective.presto.hyena.types;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.FixedWidthBlockBuilder;
import com.facebook.presto.spi.type.AbstractType;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.primitives.UnsignedLong;

public class U64Type
//        extends AbstractFixedWidthType
      extends AbstractType
      implements FixedWidthType
{
    public static final U64Type U_64_TYPE = new U64Type();

    private U64Type()
    {
      super(TypeSignature.parseTypeSignature("unsigned_long"), long.class);
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
  public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry) {
    return null;
  }

  @Override
  public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries) {
    return null;
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
        blockBuilder.writeLong(value).closeEntry();
    }

    @Override
    public int getFixedSize() {
        return 8;
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount) {
        return new FixedWidthBlockBuilder(getFixedSize(), positionCount);
    }
}
