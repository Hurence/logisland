package com.hurence.logisland.utils;

import java.io.IOException;

/**
 * Created by lhubert on 15/04/16.
 */
public interface Publisher {
    public void publish(KafkaContext context, String path, String topic) throws IOException;
}
