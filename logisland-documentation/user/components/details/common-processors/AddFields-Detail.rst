Add one or more field with constant value or dynamic value using the `expression-language <./expression-language.html>`_.Some examples of settings:

.. code::

	newStringField: bonjour
	newIntField: 14
	newIntField.field.type: INT

Would add those fields in record :

.. code::

	Field{name='newStringField', type='STRING', value='bonjour'}
	Field{name='newIntField', type='INT', value=14}

Here a second example using expression language, once for the value, once for the key. Note that you can use for both.We suppose that our record got already those fields :

.. code::

	Field{name='field1', type='STRING', value='bonjour'}
	Field{name='field2', type='INT', value=14}

This settings :
.. code::

	newStringField: ${field1 + "-" + field2}
	fieldToCalulateKey: 555
	fieldToCalulateKey.field.name: ${"_" + field1 + "-"}

Would add those fields in record :

.. code::

	Field{name='newStringField', type='STRING', value='bonjour-14'}
	Field{name='_bonjour-', type='STRING', value='555'}


As you probably notice, you can not add fields with name ending by either '.field.name' either '.field.type' because they are suffix are used to sort dynamic properties. But if you really want to do this a workaround is to specify the name of the field oui expression language, for example this settings would work:

.. code::

	fieldWithReservedSuffix: bonjour
	fieldWithReservedSuffix.field.type: INT
	fieldWithReservedSuffix.field.type: myfield.endind.with.reserved.suffix.field.type
