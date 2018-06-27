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

import co.llective.hyena.api.BlockType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;

public class HyenaColumnMetadata
        extends ColumnMetadata
{
    private BlockType blockType;

    public HyenaColumnMetadata(String name, Type type, BlockType blockType)
    {
        super(name, type, null, blockType.toString(), false);
        this.blockType = blockType;
    }

    public BlockType getBlockType()
    {
        return blockType;
    }
}