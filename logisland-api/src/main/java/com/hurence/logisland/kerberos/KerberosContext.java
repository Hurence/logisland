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
package com.hurence.logisland.kerberos;

import java.io.File;

public interface KerberosContext {

    /**
     * The Kerberos service principal used by Logisland to communicate with the KDC
     * in order to obtain tickets on behalf of Logisland. Typically of the form
     * HURENCE/fully.qualified.domain@REALM.
     *
     * @return the principal, or null if this Logisland instance is not configured
     * with a Logisland Kerberos service principal
     */
    public String getKerberosServicePrincipal();

    /**
     * The File instance for the Kerberos service keytab. The service principal
     * and service keytab will be used to communicate with the KDC to obtain
     * tickets on behalf of Logisland.
     *
     * @return the File instance of the service keytab, or null if this Logisland
     * instance is not configured with a Logisland Kerberos service keytab
     */
    public File getKerberosServiceKeytab();

    /**
     * The Kerberos configuration file (typically krb5.conf) that will be used
     * by this JVM during all Kerberos operations.
     *
     * @return the File instance for the Kerberos configuration file, or null if
     * this instance is not configured with a Kerberos configuration file
     */
    public File getKerberosConfigurationFile();
}
