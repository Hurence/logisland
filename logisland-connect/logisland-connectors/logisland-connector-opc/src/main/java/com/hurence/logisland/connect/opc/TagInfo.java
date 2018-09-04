/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.connect.opc;

import java.time.Duration;

/**
 * Information about a tag
 *
 * @author amarziali
 */
public class TagInfo {
    private final String tagId;
    private final Duration samplingInterval;
    private final StreamingMode streamingMode;

    public TagInfo(String tagId, Duration samplingInterval, StreamingMode streamingMode) {
        this.tagId = tagId;
        this.samplingInterval = samplingInterval;
        this.streamingMode = streamingMode;
    }

    public String getTagId() {
        return tagId;
    }

    public Duration getSamplingInterval() {
        return samplingInterval;
    }

    public StreamingMode getStreamingMode() {
        return streamingMode;
    }
}
