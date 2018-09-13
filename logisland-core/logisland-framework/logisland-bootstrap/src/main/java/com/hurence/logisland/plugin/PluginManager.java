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

package com.hurence.logisland.plugin;

import org.apache.ivy.Ivy;
import org.apache.ivy.core.module.id.ModuleId;
import org.apache.ivy.core.module.id.ModuleRevisionId;
import org.apache.ivy.core.resolve.ResolveOptions;
import org.apache.ivy.core.settings.IvySettings;

import java.io.File;

public class PluginManager {

    public static void main(String args[]) throws Exception {

        IvySettings settings = new IvySettings();
        if (args.length > 1) {
            settings.load(new File(args[1]));
        } else {
            settings.load(IvySettings.getDefaultSettingsURL());
        }

        Ivy ivy = Ivy.newInstance(settings);
        //ModuleRevisionId revisionId =  ModuleRevisionId.parse("com.hurence.logisland#logisland-opc-connector;0.15.0");
        ModuleRevisionId revisionId =  new ModuleRevisionId(new ModuleId("com.hurence.logisland", "logisland-connector-opc"), "0.15.0");
        System.out.println(revisionId.getRevision());
        System.out.println(ivy.resolve(revisionId, new ResolveOptions(), true));
        System.out.println(ivy.findModule(revisionId));



    }
}
