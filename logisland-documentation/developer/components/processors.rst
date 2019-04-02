.. _dev-processors:

Processors Guide
================

This document summarizes information relevant to develop a logisland Processor.

Interfaces
----------

A Logisland processor **must** implements the *com.hurence.logisland.processor.Processor* Interface.

Base of processors
------------------

For making easier the processor implementation we advise you to extends *com.hurence.logisland.processor.AbstractProcessor*. This way
most of the work is already done for you and you will benefit from future improvements.

.. note::

    If you do not extend *com.hurence.logisland.processor.AbstractProcessor*, there is several point to be carefull with.
    Read following section

Not using AbstractProcessor
+++++++++++++++++++++++++++

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

ProcessContext
++++++++++++++

See :ref:`processContext` for more information.

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

.. _proc-getSupportedDynamicPropertyDescriptor:

getSupportedDynamicPropertyDescriptor
+++++++++++++++++++++++++++++++++++++

This method is required by *AbstractProcessor* and is not required if you do not support dynamic properties.
Otherwise create here yours dynamic properties descriptions.

This property descriptor will be used to validate any user key configuration that is not in the list of supported properties.
If you return null, it is considered that the property name is not a valid dynamic property.

You can have several type of supported dynamic properties if you want as in the example below.
Go there to learn more about :ref:`user-dynamic-properties`.

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

This method should contain all initialization variables of your processor. It is called at least once before processing records.
So you can do quite heavy initialization here. But you can also use controller services as property for sharing heavy components
between different processors. You should always use a controller service for interacting with extern sources.
LINK TODO services as property

.. note::

    It is required to use at the start of the method the super.init method ! (It does some core initializing).

Example :

.. code:: java

    @Override
    public void init(ProcessContext context) {
        super.init(context);
        initDynamicProperties(context);
        this.conflictPolicy = context.getPropertyValue(CONFLICT_RESOLUTION_POLICY).asString();
    }

process
+++++++

This method is the core of the processor. This is this method that interact with Logisland Record.
It either modify them, use them, filter them or whatever you want.
Below an example that is just adding a new field to each record (this is obviously not a real processor).

.. code:: java

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        for (Record record : records) {
            record.setStringField("my_first_processor_impl", "Hello world !");
        }
        return records;
    }


Add documentation about the processor
-------------------------------------

The logisland-documentation module contains logisland documentation. See :ref:`dev-documentation-guide` for more information.
Some part of the documentation is automatically generated at build time. It uses annotation in logisland code.

In our case of a processors you have to add those :ref:`components-annotations`.

Add your processor as a logisland plugin
----------------------------------------

Unless the new processor you implemented is already in an existing logisland module you will have to do those two steps below.

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

You will have to add your module as a dependency in the **logisland-assembly** module. Add it in **full** maven profile so that it is automatically
Added to logisland jar when building with -Pfull option.

.. code:: xml

    <profile>
        <id>full</id>
        <activation>
            <activeByDefault>false</activeByDefault>
        </activation>
        <dependencies>
            ...
            <dependency>
                <groupId>com.hurence.logisland</groupId>
                <artifactId>YOUR_MODULE_NAME</artifactId>
                <version>${project.version}</version>
            </dependency>
         </dependencies>
    </profile>

