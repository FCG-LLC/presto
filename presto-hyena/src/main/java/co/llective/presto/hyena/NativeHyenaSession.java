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
import co.llective.hyena.api.ReplyException;
import co.llective.hyena.api.ScanRequest;
import co.llective.hyena.api.ScanResult;

import java.io.IOException;
import java.util.List;

public class NativeHyenaSession
        implements HyenaSession
{
    private final HyenaApi hyenaApi;

    public NativeHyenaSession(HyenaConfig config)
    {
        hyenaApi = new HyenaApi(config.getHyenaHost());
    }

    @Override
    public Catalog refreshCatalog()
    {
        try {
            return hyenaApi.refreshCatalog();
        }
        catch (IOException | ReplyException exc) {
            throw new RuntimeException("Error while refreshing catalog", exc);
        }
    }

    @Override
    public List<Column> getAvailableColumns()
    {
        return refreshCatalog().getColumns();
    }

    @Override
    public List<PartitionInfo> getAvailablePartitions()
    {
        return refreshCatalog().getAvailablePartitions();
    }

    @Override
    public ScanResult scan(ScanRequest req)
    {
        try {
            return hyenaApi.scan(req);
        }
        catch (IOException | ReplyException exc) {
            throw new RuntimeException("Error while scanning", exc);
        }
    }
}