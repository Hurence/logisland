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
package com.hurence.logisland.processor.useragent;

import nl.basjes.parse.useragent.UserAgentAnalyzer;
import nl.basjes.parse.useragent.UserAgentAnalyzer.Builder;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.util.List;


/**
 * Created by mathieu on 18/05/17.
 */
public class PooledUserAgentAnalyzerFactory extends BasePooledObjectFactory<UserAgentAnalyzer> {

    private boolean useCache = false;
    private int cacheSize = -1;
    private List<String> selectedFields;

    public PooledUserAgentAnalyzerFactory(List<String> selectedFields) {
        this.selectedFields = selectedFields;
    }

    public PooledUserAgentAnalyzerFactory(List<String> selectedFields, int cacheSize) {
        this.selectedFields = selectedFields;
        this.useCache = true;
        this.cacheSize = cacheSize;
    }

    @Override
    public UserAgentAnalyzer create() throws Exception {

        Builder builder = UserAgentAnalyzer.newBuilder();
        if (useCache) {
            builder.withCache(cacheSize);
        } else {
            builder.withoutCache();
        }
        builder.withFields(selectedFields);
        return builder.build();
    }

    @Override
    public PooledObject<UserAgentAnalyzer> wrap(UserAgentAnalyzer uaapo) {
        return new DefaultPooledObject<UserAgentAnalyzer>(uaapo);
    }

    @Override
    public void passivateObject(PooledObject<UserAgentAnalyzer> uaapo) throws Exception {
        // does nothing
    }

    @Override
    public boolean validateObject(PooledObject<UserAgentAnalyzer> uaapo) {
        // Does no check
        return true;
    }

}