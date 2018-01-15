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

import co.llective.hyena.api.Catalog;
import co.llective.hyena.api.Column;
import co.llective.hyena.api.HyenaApi;
import co.llective.hyena.api.PartitionInfo;
import co.llective.hyena.api.ScanRequest;
import co.llective.hyena.api.ScanResult;

import java.util.List;

public interface HyenaSession
{
    // We need to create new session since Nanomsg connections are not thread safe...
    NativeHyenaSession recordSetProviderSession();

    List<PartitionInfo> getAvailablePartitions();

    List<Column> getAvailableColumns();

    Catalog refreshCatalog();

    ScanResult scan(ScanRequest req, HyenaApi.HyenaOpMetadata metadataOrNull);

    void close();
}
