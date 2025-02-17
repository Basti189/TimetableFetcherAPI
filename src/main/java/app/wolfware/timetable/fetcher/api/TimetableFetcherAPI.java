package app.wolfware.timetable.fetcher.api;

import app.wolfware.timetable.fetcher.sql.DBUtils;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class TimetableFetcherAPI extends AbstractVerticle {

    public final static DateTimeFormatter formatterDate = DateTimeFormatter.ofPattern("yyMMdd");

    public TimetableFetcherAPI() {

    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        // Create a Router
        Router router = Router.router(vertx);

        router.route().handler(BodyHandler.create());
        router.get("/api/timetable/v1/:date/:train").handler(this::handleTrainRequest);

        // Create the HTTP server
        vertx.createHttpServer()
                // Handle every request using the router
                .requestHandler(router)
                // Start listening
                .listen(1435)
                // Print the port on success
                .onSuccess(server -> {
                    System.out.println("HTTP server started on port " + server.actualPort());
                    startPromise.complete();
                })
                // Print the problem on failure
                .onFailure(throwable -> {
                    throwable.printStackTrace();
                    startPromise.fail(throwable);
                });
    }

    private void handleTrainRequest(RoutingContext context) {
        String strDate = context.pathParam("date");
        String strTrain = context.pathParam("train");

        if (strTrain != null && !strTrain.isEmpty() && strDate != null && !strDate.isEmpty()) {
            int train;
            try {
                train = Integer.parseInt(strTrain);
            } catch (NumberFormatException nfe) {
                // nfe.printStackTrace();
                error(context, "Wrong train number format");
                return;
            }

            try {
                LocalDate.parse(strDate, formatterDate);
            } catch (DateTimeException dte) {
                // dte.printStackTrace();
                error(context, "Wrong date format");
                return;
            }

            String answer = DBUtils.selectrains(train, strDate);
            if (answer != null) {
                context.response()
                        .putHeader("content-type", "application/json")
                        .end(answer);
            } else {
                error(context, "train not found");
            }
        } else {
            error(context, "Ressource not found");
        }
    }

    private void error(RoutingContext context, String msg) {
        context.response()
                .setStatusCode(400)
                .putHeader("content-type", "application/json")
                .end("{\"error\": \"" + msg + "\"}");
    }
}
