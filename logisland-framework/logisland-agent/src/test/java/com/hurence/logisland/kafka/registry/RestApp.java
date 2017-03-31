package com.hurence.logisland.kafka.registry;



import com.hurence.logisland.agent.rest.RestService;
import com.hurence.logisland.avro.AvroCompatibilityLevel;
import com.hurence.logisland.kafka.registry.exceptions.RegistryException;
import com.hurence.logisland.kafka.zookeeper.RegistryIdentity;
import org.eclipse.jetty.server.Server;

import java.util.Properties;

public class RestApp {

  public final Properties prop;
  public RestService restClient;
  public KafkaRegistryRestApplication restApp;
  public Server restServer;
  public String restConnect;

  public RestApp(int port, String zkConnect, String kafkaTopic) {
    this(port, zkConnect, kafkaTopic, AvroCompatibilityLevel.NONE.name);
  }

  public RestApp(int port, String zkConnect, String kafkaTopic, String compatibilityType) {
    this(port, zkConnect, kafkaTopic, compatibilityType, true);
  }

  public RestApp(int port, String zkConnect, String kafkaTopic,
                 String compatibilityType, boolean masterEligibility) {
    prop = new Properties();
    prop.setProperty(KafkaRegistryConfig.PORT_CONFIG, ((Integer) port).toString());
    prop.setProperty(KafkaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
    prop.put(KafkaRegistryConfig.KAFKASTORE_TOPIC_JOBS_CONFIG, kafkaTopic);
    prop.put(KafkaRegistryConfig.COMPATIBILITY_CONFIG, compatibilityType);
    prop.put(KafkaRegistryConfig.MASTER_ELIGIBILITY, masterEligibility);
  }

  public void start() throws Exception {
    restApp = new KafkaRegistryRestApplication(prop);
    restServer = restApp.createServer();
    restServer.start();
    restConnect = restServer.getURI().toString();
    if (restConnect.endsWith("/"))
      restConnect = restConnect.substring(0, restConnect.length()-1);
    restClient = new RestService(restConnect);
  }

  public void stop() throws Exception {
    restClient = null;
    if (restServer != null) {
      restServer.stop();
      restServer.join();
    }
  }

  public boolean isMaster() {
    return restApp.schemaRegistry().isMaster();
  }

  public void setMaster(RegistryIdentity schemaRegistryIdentity)
      throws RegistryException {
    restApp.schemaRegistry().setMaster(schemaRegistryIdentity);
  }

  public RegistryIdentity myIdentity() {
    return restApp.schemaRegistry().myIdentity();
  }

  public RegistryIdentity masterIdentity() {
    return restApp.schemaRegistry().masterIdentity();
  }
  
  public KafkaRegistry kafkaRegistry() {
    return restApp.schemaRegistry();
  }
}
