package com.hurence.logisland.components;

import com.hurence.logisland.config.ComponentConfiguration;
import com.hurence.logisland.processor.EventProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by tom on 04/07/16.
 */
public final class ComponentsFactory {

    private static Logger logger = LoggerFactory.getLogger(ComponentsFactory.class);

    private static final AtomicLong currentId = new AtomicLong(0);


    public static  AbstractConfiguredComponent getComponent(ComponentConfiguration configuration){

        AbstractConfiguredComponent compo = null;
        switch (configuration.getType().toLowerCase()){
            case "processor":
                logger.info("creating processor {}", configuration.getProcessor());
                try {

                    File f = new File("/Users/tom/Documents/workspace/hurence/projects/log-island-hurence/logisland-assembly/target/logisland-0.9.4-bin/logisland-0.9.4/lib");
                    URL[] cp = {f.toURI().toURL()};
                    URLClassLoader urlcl = new URLClassLoader(cp);
                    Class clazz = urlcl.loadClass(configuration.getProcessor());



                    EventProcessor processor = (EventProcessor) clazz.newInstance();
                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                    logger.error("unable to instanciate processor {} : {}", configuration.getProcessor(), e.toString());
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                }

                break;
            case "engine":

                break;
            default:
                logger.error("unknown component type {}", configuration.getType());
        }
        return compo;
    }
}
