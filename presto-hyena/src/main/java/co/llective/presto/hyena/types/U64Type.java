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

    public int compareToSignedLong(long u64Value, long signedValue) {
        if (signedValue < 0) {
            return 1;
        }
        return compareUnsignedLongs(u64Value, signedValue);
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
