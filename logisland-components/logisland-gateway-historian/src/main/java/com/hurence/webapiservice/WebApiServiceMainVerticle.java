package com.hurence.webapiservice;

import com.hurence.webapiservice.historian.HistorianVerticle;
import com.hurence.webapiservice.http.HttpServerVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.rxjava.core.AbstractVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Single;

public class WebApiServiceMainVerticle extends AbstractVerticle {

  private static Logger logger = LoggerFactory.getLogger(WebApiServiceMainVerticle.class);

  private static final String CONFIG_HTTP_SERVER_ROOT = "server";
  private static final String CONFIG_HISTORIAN_ROOT = "historian";
  private static final String CONFIG_INSTANCE_NUMBER_WEB = "web.verticles.instance.number";
  private static final String CONFIG_INSTANCE_NUMBER_HISTORIAN = " historian.verticles.instance.number";

  @Override
  public void start(Promise<Void> promise) throws Exception {
    vertx.getOrCreateContext();
    Single<String> dbVerticleDeployment = deployHistorianVerticle();
    dbVerticleDeployment
            .flatMap(id -> deployHttpVerticle())
            .doOnError(promise::fail)
            .doOnSuccess(id -> promise.complete())
            .subscribe(id -> {
              logger.info("{} finished to deploy verticles", WebApiServiceMainVerticle.class.getSimpleName());
            });
  }

  private Single<String> deployHistorianVerticle() {
    int instances = config().getInteger(CONFIG_INSTANCE_NUMBER_HISTORIAN, 1);
    DeploymentOptions opts = new DeploymentOptions().setInstances(instances).setConfig(config().getJsonObject(CONFIG_HISTORIAN_ROOT));
    return vertx.rxDeployVerticle(HistorianVerticle::new, opts);
  }

  private Single<String> deployHttpVerticle() {
    int instances = config().getInteger(CONFIG_INSTANCE_NUMBER_WEB, 2);
    DeploymentOptions opts = new DeploymentOptions().setInstances(instances).setConfig(config().getJsonObject(CONFIG_HTTP_SERVER_ROOT));
    return vertx.rxDeployVerticle(HttpServerVerticle::new, opts);
  }

}
