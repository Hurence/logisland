.. _dev-documentation-guide:

Documentation Guide
===================

Here we will describe you how the doc in logisland is build and how to modify it.

Introduction
------------

The documentation in logisland is handled by **logisland-documentation** module which build
the automated part of the doc. That is why you should correctly annotate your components when developing.

All *.rst* files in this module are used to build the doc. We use *Sphinx* and *https://readthedocs.org/* for that.

So in order to change the documentation you must change these files. But do not modify files that are automatically generated !
The auto generated files are in the **components** directory. (Except for the index files)

Modify the hard coded documentation
-----------------------------------

We use ReStructuredText format for writing the doc. Then we generate html pages with `Sphinx <http://www.sphinx-doc.org>`_.
So you should be familiarized with this if you wants to do some advanced docs. Otherwise you can just modify files for minor changes.


Modify auto generated documentation
-----------------------------------

To generate generated documentation, just install the module

.. code:: sh

    cd logisland-documentation
    mvn install -DskipTests

By default, it will build all components  doc.
At the moment you must commit any modification to those files in order for it to appear on online documentation.

.. _components-annotations:

Annotation of ConfigurableComponent
+++++++++++++++++++++++++++++++++++

The auto generated documentation use annotation in code.
So be sure to add below anotations in every Component you develop.

Tags
####

It should be a list of words. So a user can rapidly filter out components. This is not currently a feature implemented
but you should still mention those tags for future use.

CapabilityDescription
#####################

This tag is used to describe the components. It should be in *.rst* format and too long. For long text please use :ref:`components-annotations-extradetailfile`

DynamicProperty
###############

This is used when your components support :ref:`user-dynamic-properties`.
You specify each property to explain how it will be used.

For example :

.. code:: java

    @DynamicProperty(name = "field to add",
        supportsExpressionLanguage = false,
        value = "default value",
        description = "Add a field to the record with the default value")

Means :

* that the name of the property will be the name of a new field created in record.
* that the value specified can support or not expression language.
* that the value will be the used as value for the new property.
* you can add a general description as well.

DynamicProperties
#################

This is used when your components support :ref:`user-dynamic-properties`.
You use thi annotation instead of **DynamicProperty** if your components support
different type of :ref:`user-dynamic-properties`.

You specify a list of annotation @DynamicProperty, one by type you support.

For example :

.. code:: java

    @DynamicProperties(value = {
        @DynamicProperty(name = "Name of the field to add",
                supportsExpressionLanguage = true,
                value = "Value of the field to add",
                description = "Add a field to the record with the specified value. Expression language can be used." +
                        "You can not add a field that end with '.type' as this suffix is used to specify the type of fields to add",
                nameForDoc = "fakeField"),
        @DynamicProperty(name = "Name of the field to add with the suffix '"+ AddFields.DYNAMIC_PROPS_TYPE_SUFFIX +"'",
                supportsExpressionLanguage = false,
                value = "Type of the field to add",
                description = "Add a field to the record with the specified type. These properties are only used if a correspondant property without" +
                        " the suffix '"+ AddFields.DYNAMIC_PROPS_TYPE_SUFFIX +"' is already defined. If this property is not defined, default type for adding fields is String." +
                        "You can only use Logisland predefined type fields.",
                nameForDoc = "fakeField" + AddFields.DYNAMIC_PROPS_TYPE_SUFFIX),
        @DynamicProperty(name = "Name of the field to add with the suffix '" + AddFields.DYNAMIC_PROPS_NAME_SUFFIX + "'",
                supportsExpressionLanguage = true,
                value = "Name of the field to add using expression language",
                description = "Add a field to the record with the specified name (which is evaluated using expression language). " +
                        "These properties are only used if a correspondant property without" +
                        " the suffix '" + AddFields.DYNAMIC_PROPS_NAME_SUFFIX + "' is already defined. If this property is not defined, " +
                        "the name of the field to add is the key of the first dynamic property (which is the main and only required dynamic property).",
                nameForDoc = "fakeField" + AddFields.DYNAMIC_PROPS_NAME_SUFFIX)
    })

.. _components-annotations-extradetailfile:

ExtraDetailFile
###############

This tag is used to add a file in *.rst* format that will be used in section 'extra information' of components documentation.
It should be a relative path, the root is ./logisland-documentation/user/components . A common path to use is :

.. code:: java

@ExtraDetailFile("./details/common-processors/AddFields-Detail.rst")

Be sure to create needed subfolder if not already exist.

ConfigurableComponent Method used
+++++++++++++++++++++++++++++++++

Each components is instantiated as a ConfigurableComponent, then we use the method :

.. code:: java

    List<PropertyDescriptor> getPropertyDescriptors();

To add information about evey supported property by the component.

.. _dev-add-doc-of-comp:

Add a ConfigurableComponent in the auto generate documentation
--------------------------------------------------------------

We have a java job **DocGenerator** which generate documentation about ConfigurableComponent in the classpath of the JVM.
Here the usage of the job :

.. code:: sh

    usage: com.hurence.logisland.documentation.DocGenerator [-a] [-d <arg>] [-f <arg>] [-h]
     -a,--append            Whether to append or replace file
     -d,--doc-dir <arg>     dir to generate documentation
     -f,--file-name <arg>   file name to generate documentation about components in classpath
     -h,--help              Print this help.

In the pom of the module we use this job several time with different parameters using the *exec-maven-plugin*.
We launch it several time with different classpath to avoid conflict issue with different version of libraries.
If you want your components documentation to be generated you have to add it in one of those executions.
If you are dealing with dependencies problem you can create a completely new execution.

For processors and services this should not be too hard as they are packaged as plugin.

For example :

.. code:: xml

    <execution>
        <id>generate doc services</id>
        <phase>install</phase>
        <configuration>
            <executable>java</executable>
            <arguments>
                <argument>-classpath</argument>
                <classpath>
                    <dependency>commons-cli:commons-cli</dependency>
                    <dependency>commons-io:commons-io</dependency>
                    <dependency>org.apache.commons:commons-lang3</dependency>
                    <dependency>org.slf4j:slf4j-simple</dependency>
                    <dependency>org.slf4j:slf4j-api</dependency>
                    <dependency>com.hurence.logisland:logisland-api</dependency>
                    <!--<dependency>com.fasterxml.jackson.core:jackson-core</dependency>-->
                    <!--<dependency>com.fasterxml.jackson.core:jackson-databind</dependency>-->
                    <dependency>com.hurence.logisland:logisland-utils</dependency>
                    <dependency>com.hurence.logisland:logisland-api</dependency>
                    <dependency>com.hurence.logisland:logisland-plugin-support</dependency>
                    <!--Needed dependencies by logisland-plugin-support-->
                    <dependency>cglib:cglib-nodep</dependency>
                    <dependency>org.springframework.boot:spring-boot-loader</dependency>
                    <!--SERVICE-->
                    <dependency>com.hurence.logisland:logisland-service-hbase_1_1_2-client</dependency>
                    <dependency>com.hurence.logisland:logisland-service-elasticsearch_2_4_0-client</dependency>
                    <dependency>com.hurence.logisland:logisland-service-elasticsearch_5_4_0-client</dependency>
                    <dependency>com.hurence.logisland:logisland-service-redis</dependency>
                    <dependency>com.hurence.logisland:logisland-service-mongodb-client</dependency>
                    <dependency>com.hurence.logisland:logisland-service-cassandra-client</dependency>
                    <dependency>com.hurence.logisland:logisland-service-solr_5_5_5-client</dependency>
                    <dependency>com.hurence.logisland:logisland-service-solr_6_6_2-client</dependency>
                    <dependency>com.hurence.logisland:logisland-service-solr_chronix_6.4.2-client</dependency>
                </classpath>
                <argument>com.hurence.logisland.documentation.DocGenerator</argument>
                <argument>-d</argument>
                <argument>${generate-components-dir}</argument>
                <argument>-f</argument>
                <argument>services</argument>
            </arguments>
        </configuration>
        <goals>
            <goal>exec</goal>
        </goals>
    </execution>

Will generate documentation for all service specified. You can just add your module in there. Then generate docs with

.. code:: sh

    mvn install -DskipTests

