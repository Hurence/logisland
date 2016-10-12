How to test your processors ?
=============

When you have coded your processor, pretty sure you want to test it with unit test.
The framawork provides you with the ``TestRunner`` tool for that.

TestRunner
----

first of all instanciate a Testrunner with your Processor and its properties.

.. code-block:: java

    final String APACHE_LOG_SCHEMA = "/schemas/apache_log.avsc";
    final String APACHE_LOG = "/data/localhost_access.log";
    final String APACHE_LOG_FIELDS =
        "src_ip,identd,user,record_time,http_method,http_query,http_version,http_status,bytes_out";
    final String APACHE_LOG_REGEX =
        "(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+\\[([\\w:/]+\\s[+\\-]\\d{4})\\]\\s+\"(\\S+)\\s+(\\S+)\\s+(\\S+)\"\\s+(\\S+)\\s+(\\S+)";

    final TestRunner testRunner = TestRunners.newTestRunner(new SplitText());
    testRunner.setProperty(SplitText.VALUE_REGEX, APACHE_LOG_REGEX);
    testRunner.setProperty(SplitText.VALUE_FIELDS, APACHE_LOG_FIELDS);
    // check if config is valid
    testRunner.assertValid();

Now enqueue some messages as if they were sent to input Kafka topics

.. code-block:: java

    testRunner.clearQueues();
    testRunner.enqueue(SplitTextTest.class.getResourceAsStream(APACHE_LOG));

Now run the process method and check that every ``Record`` has been correctly processed.

.. code-block:: java

    testRunner.run();
    testRunner.assertAllInputRecordsProcessed();
    testRunner.assertOutputRecordsCount(200);
    testRunner.assertOutputErrorCount(0);

You can validate that all output records are validated against an avro schema

.. code-block:: java

    final RecordValidator avroValidator = new AvroRecordValidator(SplitTextTest.class.getResourceAsStream
    testRunner.assertAllRecords(avroValidator);


And check if your output records behave as expected.

.. code-block:: java

    MockRecord out = testRunner.getOutputRecords().get(0);
    out.assertFieldExists("src_ip");
    out.assertFieldNotExists("src_ip2");
    out.assertFieldEquals("src_ip", "10.3.10.134");
    out.assertRecordSizeEquals(9);
    out.assertFieldEquals(FieldDictionary.RECORD_TYPE, "apache_log");
    out.assertFieldEquals(FieldDictionary.RECORD_TIME, 1469342728000L);

