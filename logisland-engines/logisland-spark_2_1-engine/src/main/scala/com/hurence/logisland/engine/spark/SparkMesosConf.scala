package com.hurence.logisland.engine.spark

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.validator.StandardValidators

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
object SparkMesosConf {


    val SPARK_MESOS_COARSE = new PropertyDescriptor.Builder()
        .name("spark.mesos.coarse")
        .description("If set to true, runs over Mesos clusters in \"coarse-grained\" sharing mode, where Spark " +
            "acquires one long-lived Mesos task on each machine. If set to false, runs over Mesos cluster" +
            " in \"fine-grained\" sharing mode, where one Mesos task is created per Spark task. " +
            "Detailed information in 'Mesos Run Modes'.")
        .required(false)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("true")
        .build



    val SPARK_MESOS_EXTRA_CORES = new PropertyDescriptor.Builder()
        .name("spark.mesos.extra.cores")
        .description("Set the extra number of cores for an executor to advertise. This does not result in more " +
            "cores allocated. It instead means that an executor will \"pretend\" it has more cores, so that the " +
            "driver will send it more tasks. Use this to increase parallelism. " +
            "This setting is only used for Mesos coarse-grained mode.")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("0")
        .build

    val SPARK_TOTAL_EXECUTOR_CORES = new PropertyDescriptor.Builder()
        .name("spark.cores.max")
        .description("When running on a standalone deploy cluster or a Mesos cluster in \"coarse-grained\" sharing mode," +
            " the maximum amount of CPU cores to request for the application from across the cluster " +
            "(not from each machine). If not set, the default will be spark.deploy.defaultCores on Spark's " +
            "standalone cluster manager, or infinite (all available cores) on Mesos.")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .build

    val SPARK_MESOS_MESOSEXECUTOR_CORES = new PropertyDescriptor.Builder()
        .name("spark.mesos.mesosExecutor.cores")
        .description("(Fine-grained mode only) Number of cores to give each Mesos executor. " +
            "This does not include the cores used to run the Spark tasks. In other words, even if no Spark task " +
            "is being run, each Mesos executor will occupy the number of cores configured here. " +
            "The value can be a floating point number.")
        .required(false)
        .addValidator(StandardValidators.FLOAT_VALIDATOR)
        .defaultValue("1.0")
        .build


    val SPARK_MESOS_EXECUTOR_DOCKER_IMAGE = new PropertyDescriptor.Builder()
        .name("spark.mesos.executor.docker.image")
        .description("Set the name of the docker image that the Spark executors will run in. " +
            "The selected image must have Spark installed, as well as a compatible version of the Mesos library. " +
            "The installed path of Spark in the image can be specified with spark.mesos.executor.home; " +
            "the installed path of the Mesos library can be specified with spark.executorEnv.MESOS_NATIVE_JAVA_LIBRARY")
        .required(false)
        .build

    val SPARK_MESOS_EXECUTOR_DOCKER_FORC_PULL_IMAGE = new PropertyDescriptor.Builder()
        .name("spark.mesos.executor.docker.forcePullImage")
        .description("Force Mesos agents to pull the image specified in spark.mesos.executor.docker.image. " +
            "By default Mesos agents will not pull images they already have cached.")
        .required(false)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("false")
        .build

    val SPARK_MESOS_EXECUTOR_DOCKER_PARAMETERS = new PropertyDescriptor.Builder()
        .name("spark.mesos.executor.docker.parameters")
        .description("Set the list of custom parameters which will be passed into the docker run command when" +
            " launching the Spark executor on Mesos using the docker containerizer. " +
            "The format of this property is a comma-separated list of key/value pairs. " +
            "Example:\nkey1=val1,key2=val2,key3=val3")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("")
        .build

    val SPARK_MESOS_EXECUTOR_DOCKER_VOLUMES = new PropertyDescriptor.Builder()
        .name("spark.mesos.executor.docker.volumes")
        .description("Set the list of volumes which will be mounted into the Docker image, which was set using " +
            "spark.mesos.executor.docker.image. The format of this property is a comma-separated list of mappings " +
            "following the form passed to docker run -v. " +
            "That is they take the form:\n[host_path:]container_path[:ro|:rw]")
        .required(false)
        .build

    val SPARK_MESOS_TASK_LABEL = new PropertyDescriptor.Builder()
        .name("spark.mesos.task.labels")
        .description("Set the Mesos labels to add to each task. Labels are free-form key-value pairs. " +
            "Key-value pairs should be separated by a colon, and commas used to list more than one." +
            " If your label includes a colon or comma, you can escape it with a backslash. Ex. key:value,key2:a\\:b.")
        .required(false)
        .build

    val SPARK_MESOS_EXECUTOR_HOME = new PropertyDescriptor.Builder()
        .name("spark.mesos.executor.home")
        .description("Set the directory in which Spark is installed on the executors in Mesos. " +
            "By default, the executors will simply use the driver's Spark home directory, " +
            "which may not be visible to them. Note that this is only relevant if a Spark binary package " +
            "is not specified through spark.executor.uri.  " +
            "default to : \tdriver side SPARK_HOME")
        .required(false)
        .build

    val SPARK_MESOS_EXECUTOR_MEMORY_OVERHEAD = new PropertyDescriptor.Builder()
        .name("spark.mesos.executor.memoryOverhead")
        .description("defaults to executor memory * 0.10, with minimum of 384\tThe amount of additional memory, specified in MB," +
            " to be allocated per executor. By default, the overhead will be larger of either 384 or 10% of " +
            "spark.executor.memory. If set, the final overhead will be this value.")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("384")
        .build

    val SPARK_MESOS_URIS = new PropertyDescriptor.Builder()
        .name("spark.mesos.uris")
        .description("A comma-separated list of URIs to be downloaded to the sandbox when driver or executor is " +
            "launched by Mesos. This applies to both coarse-grained and fine-grained mode.")
        .required(false)
        .build

    val SPARK_MESOS_ = new PropertyDescriptor.Builder()
        .name("spark.mesos.principal")
        .description("Set the principal with which Spark framework will use to authenticate with Mesos.")
        .required(false)
        .build

    val SPARK_MESOS_SECRET = new PropertyDescriptor.Builder()
        .name("spark.mesos.secret")
        .description("Set the secret with which Spark framework will use to authenticate with Mesos. " +
            "Used, for example, when authenticating with the registry.")
        .required(false)
        .build


    val SPARK_MESOS_ROLE = new PropertyDescriptor.Builder()
        .name("spark.mesos.role")
        .description("Set the role of this Spark framework for Mesos. " +
            "Roles are used in Mesos for reservations and resource weight sharing.")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("*")
        .build

    val SPARK_MESOS_CONSTRAINTS = new PropertyDescriptor.Builder()
        .name("spark.mesos.constraints")
        .description("Attribute based constraints on mesos resource offers. By default, all resource offers will" +
            " be accepted. This setting applies only to executors. Refer to Mesos Attributes & Resources for" +
            " more information on attributes.\n    Scalar constraints are matched with \"less than equal\" " +
            "semantics i.e. value in the constraint must be less than or equal to the value in the resource offer.\n   " +
            " Range constraints are matched with \"contains\" semantics i.e. value in the constraint must be " +
            "within the resource offer's value.\n    Set constraints are matched with \"subset of\" semantics " +
            "i.e. value in the constraint must be a subset of the resource offer's value.\n    " +
            "Text constraints are matched with \"equality\" semantics i.e. value in the constraint must be " +
            "exactly equal to the resource offer's value.\n    In case there is no value present as a part" +
            " of the constraint any offer with the corresponding attribute will be accepted (without value check).")
        .required(false)
        .build

    val SPARK_MESOS_DRIVER_CONSTRAINTS = new PropertyDescriptor.Builder()
        .name("spark.mesos.driver.constraints")
        .description("Same as spark.mesos.constraints except applied to drivers when launched through " +
            "the dispatcher. By default, all offers with sufficient resources will be accepted.")
        .required(false)
        .build

    val SPARK_MESOS_CONTAINERIZER = new PropertyDescriptor.Builder()
        .name("spark.mesos.containerizer")
        .description("This only affects docker containers, and must be one of \"docker\" or \"mesos\". " +
            "Mesos supports two types of containerizers for docker: the \"docker\" containerizer, and " +
            "the preferred \"mesos\" containerizer. " +
            "Read more here: http://mesos.apache.org/documentation/latest/container-image/")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("docker")
        .build


    val SPARK_MESOS_DRIVER_WEBUI_URL = new PropertyDescriptor.Builder()
        .name("spark.mesos.driver.webui.url")
        .description("Set the Spark Mesos driver webui_url for interacting with the framework. " +
            "If unset it will point to Spark's internal web UI.")
        .required(false)
        .build

    val SPARK_MESOS_DRIVER_LABELS = new PropertyDescriptor.Builder()
        .name("spark.mesos.driver.labels")
        .description("Mesos labels to add to the driver. See spark.mesos.task.labels for formatting information.")
        .required(false)
        .build

    val SPARK_MESOS_DRIVER_SECRET_VALUES = new PropertyDescriptor.Builder()
        .name("spark.mesos.driver.secret.values")
        .description("A secret is specified by its contents and destination. " +
            "These properties specify a secret's contents. To specify a secret's destination," +
            " see the cell below.\n\n    You can specify a secret's contents either (1) by value or (2) " +
            "by reference.\n\n   " +
            "(1) To specify a secret by value, set the " +
            "spark.mesos.[driver|executor].secret.values property, to make the secret available in the driver " +
            "or executors. For example, to make a secret password \"guessme\" available to the driver process, set:" +
            "spark.mesos.driver.secret.values=guessme\n    " +
            "(2) To specify a secret that has been placed in a secret store by reference, " +
            "specify its name within the secret store by setting the spark.mesos.[driver|executor].secret.names property. " +
            "For example, to make a secret password named \"password\" in a secret store available to the driver " +
            "process, set:\n\n\n    spark.mesos.driver.secret.names=password\n    Note: To use a secret store, " +
            "make sure one has been integrated with Mesos via a custom SecretResolver module.\n\n    " +
            "To specify multiple secrets, provide a comma-separated list:\n\n \n    " +
            "spark.mesos.driver.secret.values=guessme,passwd123\n    or\n    " +
            "spark.mesos.driver.secret.names=password1,password2\n    " +
            "spark.mesos.driver.secret.envkeys, spark.mesos.driver.secret.filenames, " +
            "spark.mesos.executor.secret.envkeys, spark.mesos.executor.secret.filenames,\t(none)\n    " +
            "A secret is specified by its contents and destination. These properties specify a secret's destination. " +
            "To specify a secret's contents, see the cell above.\n\n    You can specify a secret's destination " +
            "in the driver or executors as either (1) an environment variable or (2) as a file.\n\n    " +
            "(1) To make an environment-based secret, set the spark.mesos.[driver|executor].secret.envkeys property. " +
            "The secret will appear as an environment variable with the given name in the driver or executors. " +
            "For example, to make a secret password available to the driver process as $PASSWORD, set:\n\n    " +
            "spark.mesos.driver.secret.envkeys=PASSWORD\n    " +
            "(2) To make a file-based secret, set the spark.mesos.[driver|executor].secret.filenames property. " +
            "The secret will appear in the contents of a file with the given file name in the driver or executors. " +
            "For example, to make a secret password available in a file named \"pwdfile\" in the driver process, " +
            "set:\n\n    spark.mesos.driver.secret.filenames=pwdfile\n    Paths are relative to the container's " +
            "work directory. Absolute paths must already exist. Note: File-based secrets require a custom " +
            "SecretResolver module.\n\n    To specify env vars or file names corresponding to multiple secrets, " +
            "provide a comma-separated list:\n\n    spark.mesos.driver.secret.envkeys=PASSWORD1,PASSWORD2\n    " +
            "or\n    spark.mesos.driver.secret.filenames=pwdfile1,pwdfile2")
        .required(false)
        .build

    val SPARK_MESOS_DRIVER_SECRET_NAMES = new PropertyDescriptor.Builder()
        .name("spark.mesos.driver.secret.names")
        .required(false)
        .build

    val SPARK_MESOS_EXECUTOR_SECRET_VALUES = new PropertyDescriptor.Builder()
        .name("spark.mesos.executor.secret.values")
        .required(false)
        .build

    val SPARK_MESOS_EXECUTOR_SECRET_NAMES = new PropertyDescriptor.Builder()
        .name("spark.mesos.executor.secret.names")
        .required(false)
        .build


    val SPARK_MESOS_DRIVER_ENV_????? = new PropertyDescriptor.Builder()
        .name("spark.mesos.driverEnv.[EnvironmentVariableName]")
        .description("This only affects drivers submitted in cluster mode. Add the environment variable s" +
            "pecified by EnvironmentVariableName to the driver process. The user can specify multiple of these " +
            "to set multiple environment variables.")
        .required(false)
        .build

    val SPARK_MESOS_DISPATCHER_WEBUI_URL = new PropertyDescriptor.Builder()
        .name("spark.mesos.dispatcher.webui.url")
        .description("Set the Spark Mesos dispatcher webui_url for interacting with the framework. " +
            "If unset it will point to Spark's internal web UI.")
        .required(false)
        .build


    /*
        val SPARK_MESOS_ = new PropertyDescriptor.Builder()
                .name("spark.mesos.")
                .description("")
                .required(false)
                .addValidator(StandardValidators.INTEGER_VALIDATOR)
                .defaultValue("")
                .build
        spark.mesos.dispatcher.driverDefault.[PropertyName]	(none)	Set default properties for drivers submitted through the dispatcher. For example, spark.mesos.dispatcher.driverProperty.spark.executor.memory=32g results in the executors for all drivers submitted in cluster mode to run in 32g containers.
    */

    val SPARK_MESOS_DISPATCHER_HISTORY_SERVER_URL = new PropertyDescriptor.Builder()
        .name("spark.mesos.dispatcher.historyServer.url")
        .description("Set the URL of the history server. The dispatcher will then link each driver to its entry " +
            "in the history server.")
        .required(false)
        .build

    val SPARK_MESOS_GPUS_MAX = new PropertyDescriptor.Builder()
        .name("spark.mesos.gpus.max")
        .description("Set the maximum number GPU resources to acquire for this job. Note that executors will still " +
            "launch when no GPU resources are found since this configuration is just a upper limit and not a " +
            "guaranteed amount.")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("0")
        .build

    val SPARK_MESOS_NETWORK_NAME = new PropertyDescriptor.Builder()
        .name("spark.mesos.network.name")
        .description("Attach containers to the given named network. If this job is launched in cluster mode, also " +
            "launch the driver in the given named network. See the Mesos CNI docs for more details.")
        .required(false)
        .build

    val SPARK_MESOS_NETWORKS_LABELS = new PropertyDescriptor.Builder()
        .name("spark.mesos.network.labels")
        .description("Pass network labels to CNI plugins. This is a comma-separated list of key-value pairs, " +
            "where each key-value pair has the format key:value. Example:\n    key1:val1,key2:val2\n    " +
            "See the Mesos CNI docs for more details.")
        .required(false)
        .build

    val SPARK_MESOS_FETCHER_CACHE_ENABLE = new PropertyDescriptor.Builder()
        .name("spark.mesos.fetcherCache.enable")
        .description("If set to `true`, all URIs (example: `spark.executor.uri`, `spark.mesos.uris`) will be " +
            "cached by the Mesos Fetcher Cache")
        .required(false)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("false")
        .build

    val SPARK_MESOS_DRIVER_FAILOVER_TIMEOUT = new PropertyDescriptor.Builder()
        .name("spark.mesos.driver.failoverTimeout")
        .description("The amount of time (in seconds) that the master will wait for the driver to reconnect, " +
            "after being temporarily disconnected, before it tears down the driver framework by killing all its " +
            "executors. The default value is zero, meaning no timeout: if the driver disconnects, the master " +
            "immediately tears down the framework.")
        .required(false)
        .addValidator(StandardValidators.FLOAT_VALIDATOR)
        .defaultValue("0.0")
        .build

    val SPARK_MESOS_REJECT_OFFER_DURATION = new PropertyDescriptor.Builder()
        .name("spark.mesos.rejectOfferDuration")
        .description("Time to consider unused resources refused, serves as a fallback of " +
            "`spark.mesos.rejectOfferDurationForUnmetConstraints`, " +
            "`spark.mesos.rejectOfferDurationForReachedMaxCores`")
        .required(false)
        .defaultValue("120s")
        .build

    val SPARK_MESOS_REJECT_OFFER_DURATION_FOR_UNMET_CONSTRAINTS = new PropertyDescriptor.Builder()
        .name("spark.mesos.rejectOfferDurationForUnmetConstraints")
        .description("Time to consider unused resources refused with unmet constraints\n " +
            "default to : spark.mesos.rejectOfferDuration\t")
        .required(false)
        .defaultValue("120s")
        .build

    val SPARK_MESOS_REJECT_OFFER_DURATION_FOR_REACHED_MAX_CORES = new PropertyDescriptor.Builder()
        .name("spark.mesos.rejectOfferDurationForReachedMaxCores")
        .description("Time to consider unused resources refused when maximum number of cores spark.cores.max is reached")
        .required(false)
        .defaultValue("120s")
        .build

}
