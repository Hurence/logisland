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

package com.hurence.logisland.packaging;

import org.springframework.boot.loader.tools.Layout;
import org.springframework.boot.loader.tools.LayoutFactory;
import org.springframework.boot.loader.tools.Layouts;
import org.springframework.boot.loader.tools.LibraryScope;

import java.io.File;

public class LogislandPluginLayoutFactory implements LayoutFactory {

    @Override
    public Layout getLayout(File source) {
        return new Layouts.None() {
            @Override
            public String getLibraryDestination(String libraryName, LibraryScope scope) {
                if (scope.equals(LibraryScope.PROVIDED)) {
                    return null;
                }
                return super.getLibraryDestination(libraryName, scope);
            }
        };
    }
}
