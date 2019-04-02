.. _user-dynamic-properties:

Dynamic properties
==================

Overview
--------

    You use components to run jobs in logisland that manipulate records. Those components use properties that you specify in the job configuration file.
    Some of them are defined in advance by the component's developer. They got a name and you have to use it to define these properties.
    We call those properties *static properties*.

    Some components support dynamic *properties*. When this is the case, any properties specified in job conf for this component that is not
    a static property will be used as a dynamic property instead of throwing an error for a bad configuration.

    In this section we will talk about those properties and how you can use them.

Structure of a dynamic properties
---------------------------------

    Dynamic properties are really just like static properties but build on the fly. It allow to use both the name and the value of the property
    by the developer. For example instead of specifying :

.. code:: sh

    record.name: myName
    record.value: myValue

You could specify :

.. code:: sh

    myName: myValue

The advantage is that you can have any number of dynamic property whereas you have to specify in advance all static properties...


Usage of a dynamic properties
-----------------------------

    You can check the documentation of AddFields processor :ref:`user-components` that we will use in those example.

Adding a field which is concatenation of two others using '_' as joining string
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    set those dynamic properties in  AddFields processor :

    - concat2fields : ${field1 + "_" + field2}
    - my_countries : ${["france", "allemagne"]}
    - my_countries.type : array
    - my_employees_by_countries : ${["france" : 100, "allemagne" : 50]}
    - my_employees_by_countries.type : map

    Then if in input of this processor there is processors with fields : field1=value1 and field2=value2, it would have 3 more fields once
    out of this processors:

    - field 'concat2fields' of type String with value 'value1_value2'
    - field 'my_countries' of type Array containing values 'france' and 'allemagne'
    - field 'my_employees_by_countries' of type Map with key value pairs "france" : 100 and "allemagne" : 50

    By default if no type is specified by a dynamic property it use a type of String or the same type as old value if field already existed and you choose an overwrite policy.

    See AddFields processor doc :ref:`user-components` fore more information.

Conclusion
----------

    As you can see dynamic properties are very flexible but it's usage is very dependent of the implementation of the component's developer.