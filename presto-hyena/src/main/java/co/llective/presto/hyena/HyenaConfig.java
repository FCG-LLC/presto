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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class HyenaConfig
{
    private String hyenaHost = "localhost";

    private Long streamingRecordsLimit = 200000L;
    public static final String STREAMING_RECORDS_LIMIT = "streaming_records_limit";
    public static final String STREAMING_RECORDS_LIMIT_DESC = "Numbers of records fetched per subscan from hyena";

    private Long streamingRecordsThreshold = 200000L;
    public static final String STREAMING_RECORDS_THRESHOLD = "streaming_records_threshold";
    public static final String STREAMING_RECORDS_THRESHOLD_DESC = "Number of records which hyena can add/subtract to/from limit number";

    public String getHyenaHost()
    {
        return hyenaHost;
    }

    public long getStreamingRecordsLimit()
    {
        return streamingRecordsLimit;
    }

    public long getStreamingRecordsThreshold()
    {
        return streamingRecordsThreshold;
    }

    @Config("hyena.url")
    @ConfigDescription("Hyena host address")
    public HyenaConfig setHyenaHost(String hyenaHost)
    {
        this.hyenaHost = hyenaHost;
        return this;
    }

    @Config("hyena." + STREAMING_RECORDS_LIMIT)
    @ConfigDescription(STREAMING_RECORDS_LIMIT_DESC)
    public HyenaConfig setStreamingRecordsLimit(Long limit)
    {
        this.streamingRecordsLimit = limit;
        return this;
    }

    @Config("hyena." + STREAMING_RECORDS_THRESHOLD)
    @ConfigDescription(STREAMING_RECORDS_THRESHOLD_DESC)
    public HyenaConfig setStreamingRecordsThreshold(Long threshold)
    {
        this.streamingRecordsThreshold = threshold;
        return this;
    }
}
