.. _user-expression-language:

Expression Language
===================

Overview
--------

    All data in Logisland is represented by an abstraction called a Record. Those records contains fields of different types.

    You use components to run jobs in logisland that manipulate those records. Those components use properties that you specify in the job configuration file.
    Some of them support the expression language (EL). In this section we will talk about those properties and how you can use them.


Structure of a Logisland Expression
-----------------------------------

    The Logisland expression Language always begins with the start delimiter `${` and ends
    with the end delimiter `}`. Between the start and end delimiters is the text of the
    expression itself. In its most basic form, the expression can consist of just a
    record field name. For example, `${name}` will return the value of the field `name`
    of the record used.

    The use of the property depends on the implementation of the components ! Indeed it is the component
    that decide to evaluate your Logisland expression with which Record.

    For example the AddField processor use Logisland expression in its dynamic properties.

    - The key representing the name of the field to add.
    - The value can be a Logisland expression that will be used to calculate the value of the new field. In this expression you can use fields value of the current Record because it is passed as context of the Logisland expression by this processor.

    So be sure to carefully read description of the properties to understand how it will be evaluated and for what purpose.

    We are currently using the **mvel** language which you can check documentation `here <http://mvel.documentnode.com/>`_.

    .. note::

        If you want to be able to use another ScriptEngine than mvel (javascript for example). You can open an issue to ask this feature.
        Feel free to make a Pull request as well to implement this new feature.

    We have implemented some example as unit test as well if you want to check in the code source, the class is
    **com.hurence.logisland.component.TestInterpretedPropertyValueWithMvelEngine** in the module **com.hurence.logisland:logisland-api**.

    Otherwise we will show you some simple examples using the AddField processor in next Section.

Usage of a Logisland Expression
-------------------------------

    You can check the documentation of :ref:`com.hurence.logisland.processor.AddFields` processor that we will use in those example.

Adding a field which is concatenation of two others using '_' as joining string
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    set those dynamic properties in  AddFields processor :

    - concat2fields : ${field1 + "_" + field2}
    - my_countries : ${["france", "allemagne"]}
    - my_countries.type : array
    - my_employees_by_countries : ${["france" : 100, "allemagne" : 50]}
    - my_employees_by_countries.type : map

    Then if in input of this processor there is records with fields : field1=value1 and field2=value2, it would have 3 more fields once
    out of this processor:

    - field 'concat2fields' of type String with value 'value1_value2'
    - field 'my_countries' of type Array containing values 'france' and 'allemagne'
    - field 'my_employees_by_countries' of type Map with key value pairs "france" : 100 and "allemagne" : 50

    By default if no type is specified by a dynamic property it use a type of String or the same type as old value if field already existed and you choose an overwrite policy.

    See :ref:`com.hurence.logisland.processor.AddFields` processor doc for more information.

Conclusion
----------

    As you can see the language expression is very flexible but it's usage is very dependent of the implementation of the component's developer.
