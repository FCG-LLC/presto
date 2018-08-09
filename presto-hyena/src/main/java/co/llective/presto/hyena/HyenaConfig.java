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

    private Boolean streamingEnabled = true;
    public static final String STREAMING_ENABLED = "streaming_enabled";
    public static final String STREAMING_ENABLED_DESC = "Should records streaming be used";

    private Long streamingRecordsLimit = 200000L;
    public static final String STREAMING_RECORDS_LIMIT = "streaming_records_limit";
    public static final String STREAMING_RECORDS_LIMIT_DESC = "Numbers of records fetched per subscan from hyena";

    private Long streamingRecordsThreshold = 200000L;
    public static final String STREAMING_RECORDS_THRESHOLD = "streaming_records_threshold";
    public static final String STREAMING_RECORDS_THRESHOLD_DESC = "Number of records which hyena can add/subtract to/from limit number";

    private Boolean splittingEnabled = true;
    public static final String SPLITTING_ENABLED = "splitting_enabled";
    public static final String SPLITTING_ENABLED_DESC = "Should use multiple splits while scanning";

    private Integer numberOfSplits = 5;
    public static final String NUMBER_OF_SPLITS = "number_of_splits";
    public static final String NUMBER_OF_SPLITS_DESC = "Defines how many parallel scans should be fired up";

    private Long minDbTimestampNs = 1533074400000000L;  //2018-08-01
    public static final String MIN_DB_TIMESTAMP = "min_db_timestamp_ns";
    public static final String MIN_DB_TIMESTAMP_DESC = "Lowest timestamp in database in nanoseconds (only used when someone gives no constraints on time in query)";

    public String getHyenaHost()
    {
        return hyenaHost;
    }

    public boolean getStreamingEnabled()
    {
        return streamingEnabled;
    }

    public long getStreamingRecordsLimit()
    {
        return streamingRecordsLimit;
    }

    public long getStreamingRecordsThreshold()
    {
        return streamingRecordsThreshold;
    }

    public boolean getSplittingEnabled()
    {
        return splittingEnabled;
    }

    public int getNumberOfSplits()
    {
        return numberOfSplits;
    }

    public long getMinDbTimestampNs()
    {
        return minDbTimestampNs;
    }

    @Config("hyena.url")
    @ConfigDescription("Hyena host address")
    public HyenaConfig setHyenaHost(String hyenaHost)
    {
        this.hyenaHost = hyenaHost;
        return this;
    }

    @Config("hyena." + STREAMING_ENABLED)
    @ConfigDescription(STREAMING_ENABLED_DESC)
    public HyenaConfig setStreamingEnabled(Boolean streamingEnabled)
    {
        this.streamingEnabled = streamingEnabled;
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

    @Config("hyena." + SPLITTING_ENABLED)
    @ConfigDescription(SPLITTING_ENABLED_DESC)
    public HyenaConfig setSplittingEnabled(Boolean splittingEnabled)
    {
        this.splittingEnabled = splittingEnabled;
        return this;
    }

    @Config("hyena." + NUMBER_OF_SPLITS)
    @ConfigDescription(NUMBER_OF_SPLITS_DESC)
    public HyenaConfig setNumberOfSplits(Integer numberOfSplits)
    {
        this.numberOfSplits = numberOfSplits;
        return this;
    }

    @Config("hyena." + MIN_DB_TIMESTAMP)
    @ConfigDescription(MIN_DB_TIMESTAMP_DESC)
    public HyenaConfig setMinDbTimestampNs(Long timestamp)
    {
        this.minDbTimestampNs = timestamp;
        return this;
    }
}
