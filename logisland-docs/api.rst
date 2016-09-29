

API design
===================
logisland is a framework and an API, you can use it to build your own ``Processors`` or to build data processing apps over it.



The primary material : Records
----

The basic unit of processing is the Record.
A ``Record`` is a collection of ``Field``, while a ``Field`` has a ``name``, a ``type`` and a ``value``.

You can instanciate a ``Record`` like in the following code snipet:

.. code-block:: java

        String id = "firewall_record1";
        String type = "cisco";
        Record record = new Record(type);
        record.setId(id);

        assertTrue(record.isEmpty());
        assertEquals(record.size(), 3);

A record is defined by its type and a collection of fields. there three special fields:

.. code-block:: java

        // shortcut for id
        assertEquals(record.getId(), id);
        assertEquals(record.getField(Record.RECORD_ID).asString(), id);

        // shortcut for time
        assertEquals(record.getTime().getTime(),
            record.getField(Record.RECORD_TIME).asLong().longValue());

        // shortcut for type
        assertEquals(record.getType(), type);
        assertEquals(record.getType(), record.getField(Record.RECORD_TYPE).asString());
        assertEquals(record.getType(), record.getField(Record.RECORD_TYPE).getRawValue());


And the others fields have setters, getters and removers

.. code-block:: java

        record.setField("method", FieldType.STRING, "GET");
        record.setField("response_size", FieldType.INT, 452);
        record.setField("is_outside_office_hours", FieldType.BOOLEAN, false);
        record.setField("tags", FieldType.ARRAY,
            new ArrayList<>(Arrays.asList("spam", "filter", "mail")));

        assertFalse(record.hasField("unkown_field"));
        assertTrue(record.hasField("method"));
        assertEquals(record.getField("method").asString(), "GET");
        assertTrue(record.getField("response_size").asInteger() - 452 == 0);
        assertTrue(record.getField("is_outside_office_hours").asBoolean());
        record.removeField("is_outside_office_hours");
        assertFalse(record.hasField("is_outside_office_hours"));

The tools to handle processing : Processor
----

logisland is designed as a component centric framework, so there's a layer of abstraction to build configurable components.
Basically a component can be Configurable and Configured.

The most common component you'll use is the ``Processor``

Let's explain the code of a basic ``MockProcessor``, that doesn't acheive a really useful work but which is really self-explanatory
we first need to extend ``AbstractProcessor`` class (or to implement ``Processor`` interface).

.. code-block:: java

    public class MockProcessor extends AbstractProcessor {

        private static Logger logger = LoggerFactory.getLogger(MockProcessor.class);
        private static String EVENT_TYPE_NAME = "mock";

Then we have to define a list of supported ``PropertyDescriptor``. All theses properties and validation stuff are handled by
``Configurable`` interface.

.. code-block:: java

        public static final PropertyDescriptor FAKE_MESSAGE
            = new PropertyDescriptor.Builder()
                .name("fake.message")
                .description("a fake message")
                .required(true)
                .addValidator(StandardPropertyValidators.NON_EMPTY_VALIDATOR)
                .defaultValue("yoyo")
                .build();

        @Override
        public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            final List<PropertyDescriptor> descriptors = new ArrayList<>();
            descriptors.add(FAKE_MESSAGE);

            return Collections.unmodifiableList(descriptors);
        }


then comes the initialization bloc of the component given a ``ComponentContext`` (more on this later)

.. code-block:: java

    @Override
    public void init(final ComponentContext context) {
        logger.info("init MockProcessor");
    }

And now the real business part with the ``process`` method which handles all the work on the record's collection.

.. code-block:: java

    @Override
    public Collection<Record> process(final ComponentContext context,
                                      final Collection<Record> collection) {
        // log inputs
        collection.stream().forEach(record -> {
            logger.info("mock processing record : {}", record)
        });

        // output a useless record
        Record mockRecord = new Record("mock_record");
        mockRecord.setField("incomingEventsCount", FieldType.INT, collection.size());
        mockRecord.setStringField("message",
                                   context.getProperty(FAKE_MESSAGE).asString());

        return Collections.singleton(mockRecord);
    }


}


The runtime context : Instance
----
you can use your wonderful processor by setting its configuration and asking the ``ComponentFactory`` to give you one ``ProcessorInstance`` which is a ``ConfiguredComponent``.

.. code-block:: java

    String message = "logisland rocks !";
    Map<String, String> conf = new HashMap<>();
    conf.put(MockProcessor.FAKE_MESSAGE.getName(), message );

    ProcessorConfiguration componentConfiguration = new ProcessorConfiguration();
    componentConfiguration.setComponent(MockProcessor.class.getName());
    componentConfiguration.setType(ComponentType.PROCESSOR.toString());
    componentConfiguration.setConfiguration(conf);

    Optional<StandardProcessorInstance> instance =
        ComponentFactory.getProcessorInstance(componentConfiguration);
    assertTrue(instance.isPresent());

Then you need a ``ComponentContext`` to run your processor.

.. code-block:: java

    ComponentContext context = new StandardComponentContext(instance.get());
    Processor processor = instance.get().getProcessor();

And finally you can use it to process records

.. code-block:: java

    Record record = new Record("mock_record");
    record.setId("record1");
    record.setStringField("name", "tom");
    List<Record> records =
        new ArrayList<>(processor.process(context, Collections.singleton(record)));

    assertEquals(1, records.size());
    assertTrue(records.get(0).hasField("message"));
    assertEquals(message, records.get(0).getField("message").asString());



Chaining processors : ProcessorChain
----

.. warning:: @todo



Running the processor's flow : Engine
----

.. warning:: @todo




Packaging and conf
-----

The end user of logisland is not the developer, but the business analyst which does understand any line of code.
That's why we can deploy all our components through yaml config files

.. code-block:: yaml

    - processor: mock_processor
      component: com.hurence.logisland.processor.MockProcessor
      type: parser
      documentation: a parser that produce events for nothing
      configuration:
         fake.message: the super message



