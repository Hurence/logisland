package com.hurence.logisland.integration;

import java.io.IOException;

/**
 * Interface all Publishers of events should comply to in the testing framework.
 * Created by lhubert on 15/04/16.
 */
public interface Publisher {
    public void publish(KafkaContext context, String path, String topic) throws IOException;
}
