.. _dev-services:

Services
========

This document summarizes information relevant to develop a logisland controller service.

Interfaces
----------

A Logisland controller service **must** implements the *com.hurence.logisland.controller.ControllerService* Interface.

Base of controller services
---------------------------

For making easier the controller service implementation we advise you to extends *com.hurence.logisland.controller.AbstractControllerService*. This way
most of the work is already done for you and you will benefit from future improvements.

.. note::

    If you do not extend *com.hurence.logisland.controller.AbstractControllerService*, there is several point to be carefull with.
    Read following section

Not using AbstractControllerService
+++++++++++++++++++++++++++++++++++

The documentation for this part is not available yet. If you want to borrow this path, feel free to open an issue and/or talk with us on gitter
about it so we can advise you on the important point to be carefull with.

Important Object Notions
------------------------

Here we will present you the objects that you will probably have to use.

PropertyDescriptor
++++++++++++++++++

To implement a Processor you will have to add :ref:`propertyDescriptor` to your processor.
The standard way to do this is to add them as static variables of your Processor Classes. Then they will be used in the
processor's methods.

.. code:: java

    private static final AllowableValue OVERWRITE_EXISTING =
            new AllowableValue("overwrite_existing", "overwrite existing field", "if field already exist");

    private static final AllowableValue KEEP_OLD_FIELD =
            new AllowableValue("keep_only_old_field", "keep only old field value", "keep only old field");

    private static final PropertyDescriptor CONFLICT_RESOLUTION_POLICY = new PropertyDescriptor.Builder()
            .name("conflict.resolution.policy")
            .description("What to do when a field with the same name already exists ?")
            .required(false)
            .defaultValue(KEEP_OLD_FIELD.getValue())
            .allowableValues(OVERWRITE_EXISTING, KEEP_OLD_FIELD)
            .build();

ControllerServiceInitializationContext
++++++++++++++++++++++++++++++++++++++

See :ref:`controllerServiceInitializationContext` for more information.

Record
++++++

See :ref:`record` for more information.


Important methods
-----------------

Here we will present you the methods that you will probably have to implement or override.

getSupportedPropertyDescriptors
+++++++++++++++++++++++++++++++

This method is required by *AbstractProcessor*, it is used to verify that user configuration for your processor is correct.
This method should return the list of *PropertyDescriptor* that your processor supports. Be sure to add any Descriptor
provided by parents if any using super.getSupportedPropertyDescriptors() methods.

Here an example with only one supported property

.. code:: java

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(CONFLICT_RESOLUTION_POLICY);
    }


getSupportedDynamicPropertyDescriptor
+++++++++++++++++++++++++++++++++++++

This method is required by *AbstractProcessor* and is not required if you do not support dynamic properties.
Otherwise create here yours dynamic properties descriptions.

This property descriptor will be used to validate any user key configuration that is not in the list of supported properties.
If you return null, it is considered that the property name is not a valid dynamic property.

You can have several type of supported dynamic properties if you want as in the example below.

.. code:: java

     @Override
     protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        if (propertyDescriptorName.endsWith(DYNAMIC_PROPS_TYPE_SUFFIX)) {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .expressionLanguageSupported(false)
                    .addValidator(new StandardValidators.EnumValidator(FieldType.class))
                    .allowableValues(FieldType.values())
                    .defaultValue(FieldType.STRING.getName().toUpperCase())
                    .required(false)
                    .dynamic(true)
                    .build();
        }
        if (propertyDescriptorName.endsWith(DYNAMIC_PROPS_NAME_SUFFIX)) {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .expressionLanguageSupported(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .required(false)
                    .dynamic(true)
                    .build();
        }
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .required(false)
                .dynamic(true)
                .build();
    }

init
++++

This method should contain all initialization variables of your controller service. It is called at least once before you can use it.
So you can do quite heavy initialization here. You should instantiate connection with your service you want to controll so that user
of this controller can request the service without having to etablish the contact first.
Note that you should handle case where service session time out or is closed for any reason. In this case, your service should
be able to establish a connection again automatically when needed, the framework will not handle this for you.

.. note::

    It is required to use at the start of the method the super.init method ! (It does some core initializing).

Example :

.. code:: java

    @Override
    public void init(ProcessContext context) {
        super.init(context);
        this.serviceClient = buildServiceClient();
    }

Other methods defined in an API
+++++++++++++++++++++++++++++++

Services should implement an interface defining an API. For exemple *com.hurence.logisland.service.datastore.DatastoreClientService*
represents a generic api for any datastore. The advantage of using this is that a processor can work with all services implementing
this interface if it is declared as a *DatastoreClientService* instance.

For example the BulkPut processor use a *DatastoreClientService* as input so it can inject in using
any service implementing *DatastoreClientService*. So it can inject potentially in any database.

You can create a special module to create a desired interface that you want your service to implement. This way other services
would be able to use it as well.

Here a method for example defined in *DatastoreClientService*.

.. code:: java

    /**
     * Drop the specified collection/index/table/bucket.
     * Specify namespace as dotted notation like in `global.users`
     */
    void dropCollection(String name)throws DatastoreClientServiceException;


Add documentation about the service
-----------------------------------

The logisland-documentation module contains logisland documentation. See :ref:`dev-documentation-guide` for more information.
Some part of the documentation is automatically generated at build time. It uses annotation in logisland code.

In our case of a service you have to add those :ref:`components-annotations`.

Also you need to add your module dependency in documentation module like explained here :ref:`dev-add-doc-of-comp`.

Add your service as a logisland plugin
----------------------------------------

Unless the new service you implemented is already in an existing logisland module you will have to do those two steps below.

Make your module a logisland plugin container
+++++++++++++++++++++++++++++++++++++++++++++

You will have to build your module as a plugin in two steps :
* Using **spring-boot-maven-plugin** that will build a fat jar of your module.
* Using our custom plugin **logisland-maven-plugin** that will modify the manifest of the jar so that logisland get some meta information.

You just have to add this code in the *pom.xml* of your module.

.. code:: xml

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <executions>
                    <execution>
                        <phase>package</phase>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>com.hurence.logisland</groupId>
                <artifactId>logisland-maven-plugin</artifactId>
                <executions>
                    <execution>
                        <phase>package</phase>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>

Add your module in tar gz assembly
++++++++++++++++++++++++++++++++++

You will have to add your module in the ./logisland-assembly/src/assembly/full-assembly.xml descriptor in the **logisland-assembly** module.
So that it is automatically embedded to logisland full tar gz version.

.. code:: xml

<moduleSets>
    <moduleSet>
        <!-- Enable access to all projects in the current multimodule build! -->
        <useAllReactorProjects>true</useAllReactorProjects>

        <!-- Now, select which projects to include in this module-set. -->
        <includes>
            <!--                SERVICES          -->
            <include>com.hurence.logisland:logisland-service-inmemory-cache</include>
            <include>com.hurence.logisland:YOUR_MODULE_NAME</include>
            ...
            <!--                PROCESSORS          -->
            <include>com.hurence.logisland:logisland-processor-common</include>
            ...
            <!--                CONNECTORS          -->
            <include>com.hurence.logisland:logisland-connector-opc</include>
            ...
        </includes>
        <binaries>
            <directoryMode>0770</directoryMode>
            <fileMode>0660</fileMode>
            <outputDirectory>lib/plugins</outputDirectory>
            <includeDependencies>false</includeDependencies>
            <unpack>false</unpack>
        </binaries>
    </moduleSet>
</moduleSets>

