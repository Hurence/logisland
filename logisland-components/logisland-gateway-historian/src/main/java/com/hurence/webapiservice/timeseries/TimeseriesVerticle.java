
package com.hurence.webapiservice.timeseries;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.serviceproxy.ServiceBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TimeseriesVerticle extends AbstractVerticle {

  private static Logger logger = LoggerFactory.getLogger(TimeseriesVerticle.class);


  public static final String CONFIG_TIMESERIES_ADDRESS = "address";

  @Override
  public void start(Promise<Void> promise) throws Exception {
    final String address = config().getString(CONFIG_TIMESERIES_ADDRESS, "timeseries");

    TimeseriesService.create(vertx, ready -> {
      if (ready.succeeded()) {
        ServiceBinder binder = new ServiceBinder(vertx);
        binder.setAddress(address)
                .register(TimeseriesService.class, ready.result());
        logger.trace("{} deployed on address : '{}'", TimeseriesService.class.getSimpleName(), address);
        promise.complete();
      } else {
        promise.fail(ready.cause());
      }
    });
  }
}
