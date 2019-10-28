package com.hurence.webapiservice.http.grafana;


import com.hurence.logisland.record.FieldDictionary;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;

public interface GrafanaApi {

    default Router getGraphanaRouter(Vertx vertx) {
        Router router = Router.router(vertx);
        router.get("/").handler(this::rootHandler);
        router.get("/search").handler(this::searchHandler);
        router.get("/query").handler(this::queryHandler);
        router.get("/annotations").handler(this::annotationsHandler);
        router.get("/tag-keys").handler(this::tagKeysHandler);
        router.get("/tag-values").handler(this::tagValuesHandler);
        return router;
    }

    /**
     * should return 200 ok
     * @param context
     */
    void rootHandler(RoutingContext context);

    /**
     *  used by the find metric options on the query tab in panels.
     *  In our case we will return each different '{@value FieldDictionary#RECORD_NAME}' value in historian.
     * @param context
     * Expected request exemple :
     * <pre>
     * { target: 'upper_50' }
     * </pre>
     * response Exemple :
     * <pre>
     *     ["upper_25","upper_50","upper_75","upper_90","upper_95"]
     * </pre>
     *
     * @see <a href="https://grafana.com/grafana/plugins/grafana-simple-json-datasource.">
     *          https://grafana.com/grafana/plugins/grafana-simple-json-datasource.
     *      </a>
     *
     *
     */
    void searchHandler(RoutingContext context);

    /**
     * should return metrics based on input.
     * @param context
     */
    void queryHandler(RoutingContext context);

    /**
     * should return annotations.
     * @param context
     */
    void annotationsHandler(RoutingContext context);

    /**
     * should return tag keys for ad hoc filters.
     * @param context
     */
    void tagKeysHandler(RoutingContext context);

    /**
     * should return tag values for ad hoc filters.
     * @param context
     */
    void tagValuesHandler(RoutingContext context);
}
