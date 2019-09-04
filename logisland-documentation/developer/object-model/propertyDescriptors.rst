.. _propertyDescriptor:

PropertyDescriptors
===================

This document summarizes information relevant for using *com.hurence.logisland.component.PropertyDescriptor*
which is part of Logisland api and is used throughout Logisland.

Purpose
-------

This object is used to describe a property that users can used in job configuration when using a component.
In a component, you will describe those properties using *com.hurence.logisland.component.PropertyDescriptor*.

Builder
-------

You create a PropertyDescriptor using the builder this way :

.. code:: java

    private static final PropertyDescriptor CONFLICT_RESOLUTION_POLICY = new PropertyDescriptor.Builder()
            .name("conflict.resolution.policy")
            .description("What to do when a field with the same name already exists ?")
            .required(false)
            .defaultValue(KEEP_OLD_FIELD.getValue())
            .allowableValues("value1", "value2")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

You can use

.. code:: java

    .identifiesControllerService(ElasticsearchClientService.class)

When you want a property to be used to reference a :ref:`dev-services`

properties
++++++++++

Here we will describe each element you can set to a PropertyDescriptor.

name
____

This is the string that will be used by the client in the yaml conf file.

description
___________

This is used in the auto generated documentation of components to describe properties.

required
________

If this property is mandatory or not

defaultValue
____________

Default value if any

allowableValues
_______________

To specify a specific set of authorized values (Add a constraint on the expected value of the property).

expressionLanguageSupported
___________________________

Specify if :ref:`user-expression-language` is supported for this property or not.

addValidator
____________

Add given validator to the property (Add a constraint on the expected value of the property).

sensitive
_________

Specifies if the property contain sensitive information or not.

