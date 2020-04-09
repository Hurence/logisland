package com.hurence.logisland.stream.spark.structured.provider;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import com.hurence.logisland.stream.StreamProperties;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.fail;

public class AzureEventHubsStructuredStreamProviderServiceTest {

    @Test
    public void testConfig() throws InitializationException {

        boolean error = false;
        // Missing namespace
        try {
            error = false;
            final AzureEventHubsStructuredStreamProviderService service =
                    new AzureEventHubsStructuredStreamProviderService();
            // Any processor will do it, we won't use it but we need a real processor to be instantiated
            final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.datastore.BulkPut");
            runner.addControllerService("eventhubs_service", service);
            runner.enableControllerService(service);
            error = true;
            fail("Namespace not defined: this should have failed");
        } catch (AssertionError e) {
            if (error) {
                fail(e.getMessage());
            } else {
                System.out.println(e.getMessage());
            }
        }

        // Namespace but missing read or write hub
        try {
            error = false;
            final AzureEventHubsStructuredStreamProviderService service =
                    new AzureEventHubsStructuredStreamProviderService();
            final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.datastore.BulkPut");
            runner.addControllerService("eventhubs_service", service);
            runner.setProperty(service, StreamProperties.EVENTHUBS_NAMESPACE().getName(), "namespace");
            runner.enableControllerService(service);
            error = true;
            fail("Namespace defined but missing read or write hub: this should have failed");
        } catch (AssertionError e) {
            if (error) {
                fail(e.getMessage());
            } else {
                System.out.println(e.getMessage());
            }
        }

        /**
         * READ EVENT HUB ONLY
         */

        // Namespace, read hub but missing sasKeyName
        try {
            error = false;
            final AzureEventHubsStructuredStreamProviderService service =
                    new AzureEventHubsStructuredStreamProviderService();
            final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.datastore.BulkPut");
            runner.addControllerService("eventhubs_service", service);
            runner.setProperty(service, StreamProperties.EVENTHUBS_NAMESPACE().getName(), "namespace");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_EVENT_HUB().getName(), "read_hub");
            runner.enableControllerService(service);
            error = true;
            fail("Read hub defined but missing sasKeyName: this should have failed");
        } catch (AssertionError e) {
            if (error) {
                fail(e.getMessage());
            } else {
                System.out.println(e.getMessage());
            }
        }

        // Namespace, read hub, sasKeyName but missing sasKey
        try {
            error = false;
            final AzureEventHubsStructuredStreamProviderService service =
                    new AzureEventHubsStructuredStreamProviderService();
            final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.datastore.BulkPut");
            runner.addControllerService("eventhubs_service", service);
            runner.setProperty(service, StreamProperties.EVENTHUBS_NAMESPACE().getName(), "namespace");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_EVENT_HUB().getName(), "read_hub");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_SAS_KEY_NAME().getName(), "read_sas_key_name");
            runner.enableControllerService(service);
            error = true;
            fail("Read hub defined, sasKeyName defined but missing sasKey: this should have failed");
        } catch (AssertionError e) {
            if (error) {
                fail(e.getMessage());
            } else {
                System.out.println(e.getMessage());
            }
        }

        // Namespace, read hub, sasKeyName and sasKey -> should be ok
        try {
            final AzureEventHubsStructuredStreamProviderService service =
                    new AzureEventHubsStructuredStreamProviderService();
            final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.datastore.BulkPut");
            runner.addControllerService("eventhubs_service", service);
            runner.setProperty(service, StreamProperties.EVENTHUBS_NAMESPACE().getName(), "namespace");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_EVENT_HUB().getName(), "read_hub");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_SAS_KEY_NAME().getName(), "read_sas_key_name");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_SAS_KEY().getName(), "read_sas_key");
            runner.enableControllerService(service);
            error = true;
            System.out.println("Read hub defined, sasKeyName, sasKey defined: ok");
        } catch (AssertionError e) {
            fail("Read hub defined, sasKeyName, sasKey defined: this should have passed");
        }

        // Bad read position value
        try {
            error = false;
            final AzureEventHubsStructuredStreamProviderService service =
                    new AzureEventHubsStructuredStreamProviderService();
            final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.datastore.BulkPut");
            runner.addControllerService("eventhubs_service", service);
            runner.setProperty(service, StreamProperties.EVENTHUBS_NAMESPACE().getName(), "namespace");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_EVENT_HUB().getName(), "read_hub");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_SAS_KEY_NAME().getName(), "read_sas_key_name");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_SAS_KEY().getName(), "read_sas_key");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_POSITION().getName(), "bad 0123456789 value");
            runner.enableControllerService(service);
            error = true;
            fail("Bad read position value: this should have failed");
        } catch (AssertionError e) {
            if (error) {
                fail(e.getMessage());
            } else {
                System.out.println(e.getMessage());
            }
        }

        // Bad read position type
        try {
            error = false;
            final AzureEventHubsStructuredStreamProviderService service =
                    new AzureEventHubsStructuredStreamProviderService();
            final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.datastore.BulkPut");
            runner.addControllerService("eventhubs_service", service);
            runner.setProperty(service, StreamProperties.EVENTHUBS_NAMESPACE().getName(), "namespace");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_EVENT_HUB().getName(), "read_hub");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_SAS_KEY_NAME().getName(), "read_sas_key_name");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_SAS_KEY().getName(), "read_sas_key");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_POSITION_TYPE().getName(), "bad value");
            runner.enableControllerService(service);
            error = true;
            fail("Bad read position type value: this should have failed");
        } catch (AssertionError e) {
            if (error) {
                fail(e.getMessage());
            } else {
                System.out.println(e.getMessage());
            }
        }

        // Set all read properties
        try {
            final AzureEventHubsStructuredStreamProviderService service =
                    new AzureEventHubsStructuredStreamProviderService();
            final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.datastore.BulkPut");
            runner.addControllerService("eventhubs_service", service);
            runner.setProperty(service, StreamProperties.EVENTHUBS_NAMESPACE().getName(), "namespace");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_EVENT_HUB().getName(), "read_hub");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_SAS_KEY_NAME().getName(), "read_sas_key_name");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_SAS_KEY().getName(), "read_sas_key");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_POSITION_TYPE().getName(), "sequenceNumber");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_CONSUMER_GROUP().getName(), "consumerGroup");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_RECEIVER_TIMEOUT().getName(), "123");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_PREFETCH_COUNT().getName(), "456");
            runner.enableControllerService(service);
            System.out.println("All read properties set: ok");
        } catch (AssertionError e) {
            fail("All read properties set: this should have passed");
        }

        /**
         * WRITE EVENT HUB ONLY
         */

        // Namespace, write hub but missing sasKeyName
        try {
            error = false;
            final AzureEventHubsStructuredStreamProviderService service =
                    new AzureEventHubsStructuredStreamProviderService();
            final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.datastore.BulkPut");
            runner.addControllerService("eventhubs_service", service);
            runner.setProperty(service, StreamProperties.EVENTHUBS_NAMESPACE().getName(), "namespace");
            runner.setProperty(service, StreamProperties.EVENTHUBS_WRITE_EVENT_HUB().getName(), "write_hub");
            runner.enableControllerService(service);
            error = true;
            fail("Write hub defined but missing sasKeyName: this should have failed");
        } catch (AssertionError e) {
            if (error) {
                fail(e.getMessage());
            } else {
                System.out.println(e.getMessage());
            }
        }

        // Namespace, write hub, sasKeyName but missing sasKey
        try {
            error = false;
            final AzureEventHubsStructuredStreamProviderService service =
                    new AzureEventHubsStructuredStreamProviderService();
            final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.datastore.BulkPut");
            runner.addControllerService("eventhubs_service", service);
            runner.setProperty(service, StreamProperties.EVENTHUBS_NAMESPACE().getName(), "namespace");
            runner.setProperty(service, StreamProperties.EVENTHUBS_WRITE_EVENT_HUB().getName(), "write_hub");
            runner.setProperty(service, StreamProperties.EVENTHUBS_WRITE_SAS_KEY_NAME().getName(), "write_sas_key_name");
            runner.enableControllerService(service);
            error = true;
            fail("Write hub defined, sasKeyName defined but missing sasKey: this should have failed");
        } catch (AssertionError e) {
            if (error) {
                fail(e.getMessage());
            } else {
                System.out.println(e.getMessage());
            }
        }

        // Namespace, write hub, sasKeyName and sasKey -> should be ok
        try {
            final AzureEventHubsStructuredStreamProviderService service =
                    new AzureEventHubsStructuredStreamProviderService();
            final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.datastore.BulkPut");
            runner.addControllerService("eventhubs_service", service);
            runner.setProperty(service, StreamProperties.EVENTHUBS_NAMESPACE().getName(), "namespace");
            runner.setProperty(service, StreamProperties.EVENTHUBS_WRITE_EVENT_HUB().getName(), "write_hub");
            runner.setProperty(service, StreamProperties.EVENTHUBS_WRITE_SAS_KEY_NAME().getName(), "write_sas_key_name");
            runner.setProperty(service, StreamProperties.EVENTHUBS_WRITE_SAS_KEY().getName(), "write_sas_key");
            runner.enableControllerService(service);
            System.out.println("Write hub defined, sasKeyName, sasKey defined: ok");
        } catch (AssertionError e) {
            fail("Write hub defined, sasKeyName, sasKey defined: this should have passed");
        }

        /**
         * BOTH READ AND WRITE EVENT HUBS
         */

        // Both read and write hubs, minimum set of properties needed
        try {
            final AzureEventHubsStructuredStreamProviderService service =
                    new AzureEventHubsStructuredStreamProviderService();
            final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.datastore.BulkPut");
            runner.addControllerService("eventhubs_service", service);
            runner.setProperty(service, StreamProperties.EVENTHUBS_NAMESPACE().getName(), "namespace");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_EVENT_HUB().getName(), "read_hub");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_SAS_KEY_NAME().getName(), "read_sas_key_name");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_SAS_KEY().getName(), "read_sas_key");
            runner.setProperty(service, StreamProperties.EVENTHUBS_WRITE_EVENT_HUB().getName(), "write_hub");
            runner.setProperty(service, StreamProperties.EVENTHUBS_WRITE_SAS_KEY_NAME().getName(), "write_sas_key_name");
            runner.setProperty(service, StreamProperties.EVENTHUBS_WRITE_SAS_KEY().getName(), "write_sas_key");
            runner.enableControllerService(service);
            System.out.println("Read and Write hub defined with their key properties defined: ok");
        } catch (AssertionError e) {
            fail("Read and Write hub defined with their key properties defined: this should have passed");
        }

        // Bad read position value as long -> ok
        try {
            final AzureEventHubsStructuredStreamProviderService service =
                    new AzureEventHubsStructuredStreamProviderService();
            final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.datastore.BulkPut");
            runner.addControllerService("eventhubs_service", service);
            runner.setProperty(service, StreamProperties.EVENTHUBS_NAMESPACE().getName(), "namespace");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_EVENT_HUB().getName(), "read_hub");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_SAS_KEY_NAME().getName(), "read_sas_key_name");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_SAS_KEY().getName(), "read_sas_key");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_POSITION().getName(), "1234");
            runner.enableControllerService(service);
            System.out.println("Read position is a long: ok");
        } catch (AssertionError e) {
            fail("Read position as long should haven been ok");
        }

        // Set all possible read and write properties
        try {
            final AzureEventHubsStructuredStreamProviderService service =
                    new AzureEventHubsStructuredStreamProviderService();
            final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.datastore.BulkPut");
            runner.addControllerService("eventhubs_service", service);
            runner.setProperty(service, StreamProperties.EVENTHUBS_NAMESPACE().getName(), "namespace");
            runner.setProperty(service, StreamProperties.EVENTHUBS_MAX_EVENTS_PER_TRIGGER().getName(), "987");
            runner.setProperty(service, StreamProperties.EVENTHUBS_OPERATION_TIMEOUT().getName(), "654");
            runner.setProperty(service, StreamProperties.EVENTHUBS_THREAD_POOL_SIZE().getName(), "321");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_EVENT_HUB().getName(), "read_hub");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_SAS_KEY_NAME().getName(), "read_sas_key_name");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_SAS_KEY().getName(), "read_sas_key");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_POSITION().getName(), "8963");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_POSITION_TYPE().getName(), "offset");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_CONSUMER_GROUP().getName(), "consumerGroup");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_RECEIVER_TIMEOUT().getName(), "8436");
            runner.setProperty(service, StreamProperties.EVENTHUBS_READ_PREFETCH_COUNT().getName(), "4723");
            runner.setProperty(service, StreamProperties.EVENTHUBS_WRITE_EVENT_HUB().getName(), "write_hub");
            runner.setProperty(service, StreamProperties.EVENTHUBS_WRITE_SAS_KEY_NAME().getName(), "write_sas_key_name");
            runner.setProperty(service, StreamProperties.EVENTHUBS_WRITE_SAS_KEY().getName(), "write_sas_key");
            runner.enableControllerService(service);
            System.out.println("All read and write properties set: ok");
        } catch (AssertionError e) {
            fail("All read and write properties set: this should have passed");
        }
    }
}
