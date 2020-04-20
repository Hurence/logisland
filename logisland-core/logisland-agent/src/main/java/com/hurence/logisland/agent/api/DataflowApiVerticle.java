package com.hurence.logisland.agent.api;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import com.hurence.logisland.agent.api.model.DataFlow;

import java.util.List;
import java.util.Map;

public class DataflowApiVerticle extends AbstractVerticle {
    final static Logger LOGGER = LoggerFactory.getLogger(DataflowApiVerticle.class); 
    
    final static String NOTIFYDATAFLOWCONFIGURATION_SERVICE_ID = "notifyDataflowConfiguration";
    final static String POLLDATAFLOWCONFIGURATION_SERVICE_ID = "pollDataflowConfiguration";
    
    //TODO : create Implementation
    DataflowApi service = new DataflowApiImpl();

    @Override
    public void start() throws Exception {
        
        //Consumer for notifyDataflowConfiguration
        vertx.eventBus().<JsonObject> consumer(NOTIFYDATAFLOWCONFIGURATION_SERVICE_ID).handler(message -> {
            try {
                String dataflowName = message.body().getString("dataflowName");
                String jobId = message.body().getString("jobId");
                DataFlow dataflow = Json.mapper.readValue(message.body().getJsonObject("dataflow").encode(), DataFlow.class);
                
                service.notifyDataflowConfiguration(dataflowName, jobId, dataflow);
                message.reply(null);
            } catch (Exception e) {
                //TODO : replace magic number (101)
                message.fail(101, e.getLocalizedMessage());
            }
        });
        
        //Consumer for pollDataflowConfiguration
        vertx.eventBus().<JsonObject> consumer(POLLDATAFLOWCONFIGURATION_SERVICE_ID).handler(message -> {
            try {
                String dataflowName = message.body().getString("dataflowName");
                String ifModifiedSince = message.body().getString("If-Modified-Since");
                
                DataFlow result = service.pollDataflowConfiguration(dataflowName, ifModifiedSince);
                message.reply(new JsonObject(Json.encode(result)).encodePrettily());
            } catch (Exception e) {
                //TODO : replace magic number (101)
                message.fail(101, e.getLocalizedMessage());
            }
        });
        
    }
}