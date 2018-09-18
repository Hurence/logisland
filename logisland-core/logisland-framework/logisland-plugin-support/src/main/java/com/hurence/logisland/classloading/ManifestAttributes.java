/*
 *  * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.classloading;

import java.util.jar.Attributes;

public interface ManifestAttributes {

    Attributes.Name MODULE_EXPORTS = new Attributes.Name("Logisland-Module-Exports");
    Attributes.Name MODULE_VERSION = new Attributes.Name("Logisland-Module-Version");
    Attributes.Name CLASSLOADER_PARENT_FIRST = new Attributes.Name("Classloader-Parent-First");

}
