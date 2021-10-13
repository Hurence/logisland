/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.swagger.server.api;

import java.nio.charset.Charset;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.phiz71.vertx.swagger.router.OperationIdServiceIdResolver;
import com.github.phiz71.vertx.swagger.router.SwaggerRouter;

import io.swagger.models.Swagger;
import io.swagger.parser.SwaggerParser;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.file.FileSystem;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;

public class MainApiVerticle extends AbstractVerticle {
    final static Logger LOGGER = LoggerFactory.getLogger(MainApiVerticle.class); 
    
    final Router router = Router.router(vertx);
    
    @Override
    public void start(Future<Void> startFuture) throws Exception {
        Json.mapper.registerModule(new JavaTimeModule());
    	FileSystem vertxFileSystem = vertx.fileSystem();
        vertxFileSystem.readFile("swagger.json", readFile -> {
            if (readFile.succeeded()) {
                Swagger swagger = new SwaggerParser().parse(readFile.result().toString(Charset.forName("utf-8")));
                Router swaggerRouter = SwaggerRouter.swaggerRouter(Router.router(vertx), swagger, vertx.eventBus(), new OperationIdServiceIdResolver());
            
                deployVerticles(startFuture);
                
                vertx.createHttpServer() 
                    .requestHandler(swaggerRouter::accept) 
                    .listen(8081);
                startFuture.complete();
            } else {
            	startFuture.fail(readFile.cause());
            }
        });        		        
    }
      
    public void deployVerticles(Future<Void> startFuture) {
        
        vertx.deployVerticle("com.hurence.logisland.agent.api.DataflowApiVerticle", res -> {
            if (res.succeeded()) {
                LOGGER.info("DataflowApiVerticle : Deployed");
            } else {
                startFuture.fail(res.cause());
                LOGGER.error("DataflowApiVerticle : Deployement failed");
            }
        });
        
    }
}