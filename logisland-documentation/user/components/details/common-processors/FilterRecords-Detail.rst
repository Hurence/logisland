Here we will show some example that filter records using dynamic properties:
Those dynamic properties support the `expression-language <./expression-language.html>`_.Some examples of settings:

.. code::

	logic: OR
	age_older_than_18: "${return age > 18}"
	name_starting_by_GR: "${return name.startsWith(\"GR\")}"
	complex_condition: "${return name.contains(\"jo\") && age > 25 && mother == \"jocelyne\" || joker == true}"

If we had those records in input :

.. code::
    Record1
    Field{name='name', type='STRING', value='bonjour'}
    Field{name='mother', type='STRING', value='bonjour'}
    Field{name='age', type='INT', value=29}
    Field{name='joker', type='BOOLEAN', value=false}

	Record2
    Field{name='name', type='STRING', value='bonjour'}
    Field{name='mother', type='STRING', value='bonjour'}
    Field{name='age', type='INT', value=14}
    Field{name='joker', type='BOOLEAN', value=false}

    Record3
    Field{name='name', type='STRING', value='bonjour'}
    Field{name='mother', type='STRING', value='bonjour'}
    Field{name='age', type='INT', value=14}
    Field{name='joker', type='BOOLEAN', value=true}

In output we would get Record1 and Record3 as Record2 does not verify any of those conditions.


