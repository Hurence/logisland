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
/**
  * Copyright (C) 2016 Hurence (support@hurence.com)
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.hurence.logisland.stream.spark.structured.provider;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
  *
  * @author bailett
  */
@CapabilityDescription("Provide a way to read a local file as input in StructuredStream streams")
public class LocalFileStructuredStreamProviderServiceTest {

  private Logger logger = LoggerFactory.getLogger(LocalFileStructuredStreamProviderServiceTest.class);

  private String JOB_CONF_FILE = "/conf/timeseries-structured-stream.yml";


  @Test
  @Ignore
  public void testLocalFileStructuredStreamProviderService() {
    ProviderServiceAsReaderRunner runner = new ProviderServiceAsReaderRunner(null);
    runner.run();
  }

}