package com.hurence.logisland.components;

import com.hurence.logisland.config.ComponentConfiguration;
import com.hurence.logisland.processor.EventProcessor;
import com.hurence.logisland.processor.StandardProcessorInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by tom on 04/07/16.
 */
public final class ComponentsFactory {

    private static Logger logger = LoggerFactory.getLogger(ComponentsFactory.class);

    private static final AtomicLong currentId = new AtomicLong(0);


    public static StandardProcessorInstance getProcessorInstance(ComponentConfiguration configuration) {

        switch (configuration.getType().toLowerCase()) {
            case "processor":

                try {
                    final EventProcessor processor = (EventProcessor) Class.forName(configuration.getProcessor()).newInstance();
                    final StandardProcessorInstance instance = new StandardProcessorInstance(processor, Long.toString(currentId.incrementAndGet()));


                    configuration.getConfiguration()
                            .entrySet()
                            .stream()
                            .forEach(e -> instance.setProperty(e.getKey(), e.getValue()));

                    logger.info("created processor {}", configuration.getProcessor());

                    return instance;

                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                    logger.error("unable to instanciate processor {} : {}", configuration.getProcessor(), e.toString());
                }

                break;
            case "engine":

                break;
            default:
                logger.error("unknown component type {}", configuration.getType());
        }
        return null;
    }
}
