package com.hurence.webapiservice.http.grafana;


import com.hurence.logisland.record.FieldDictionary;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;

public interface GrafanaApi {

    default Router getGraphanaRouter(Vertx vertx) {
        Router router = Router.router(vertx);
        router.get("/").handler(this::root);
        router.post("/search").handler(this::search);
        router.post("/query")
                .produces("application/json")
                .handler(this::query);
        router.post("/annotations").handler(this::annotations);
        router.post("/tag-keys").handler(this::tagKeys);
        router.post("/tag-values").handler(this::tagValues);
        return router;
    }

    /**
     * should return 200 ok
     * @param context
     */
    void root(RoutingContext context);

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
    void search(RoutingContext context);

    /**
     * should return metrics based on input.
     * @param context
     */
    void query(RoutingContext context);

    /**
     * should return annotations.
     * @param context
     */
    void annotations(RoutingContext context);

    /**
     * should return tag keys for ad hoc filters.
     * @param context
     */
    void tagKeys(RoutingContext context);

    /**
     * should return tag values for ad hoc filters.
     * @param context
     */
    void tagValues(RoutingContext context);
}
