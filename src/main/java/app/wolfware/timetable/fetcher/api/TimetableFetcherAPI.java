package app.wolfware.timetable.fetcher.api;

import app.wolfware.timetable.fetcher.sql.DBUtils;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.zip.GZIPOutputStream;

public class TimetableFetcherAPI extends AbstractVerticle {

    public final static DateTimeFormatter formatterDate = DateTimeFormatter.ofPattern("yyMMdd");

    public TimetableFetcherAPI() {

    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        // Create a Router
        Router router = Router.router(vertx);

        router.route().handler(BodyHandler.create());
        router.get("/api/timetable/v1/train").handler(this::handleTrainRequest);

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
        String strDate = context.request().getParam("date");
        String strTrain = context.request().getParam("number");
        String strMeta = context.request().getParam("meta");

        if (strTrain != null && !strTrain.isEmpty()) {
            int train;
            try {
                train = Integer.parseInt(strTrain);
            } catch (NumberFormatException nfe) {
                // nfe.printStackTrace();
                error(context, "Wrong train number format");
                return;
            }

            if (strDate != null && !strDate.isEmpty()) {
                try {
                    LocalDate.parse(strDate, formatterDate);
                } catch (DateTimeException dte) {
                    // dte.printStackTrace();
                    error(context, "Wrong date format");
                    return;
                }
            }

            if (strMeta != null && !strMeta.isEmpty()) {
                try {
                    int m = Integer.parseInt(strMeta);
                    if (!(m == 0 || m == 1)) {
                        strMeta = null;
                    }
                } catch (NumberFormatException nfe) {
                    strMeta = null;
                }
            } else {
                strMeta = null;
            }

            String answer = DBUtils.selectTrains(train, strDate, strMeta);

            HttpServerResponse response = context.response();
            response.putHeader("Content-Type", "application/json");

            if (answer != null) {
                if (context.request().getHeader("Accept-Encoding") != null &&
                        context.request().getHeader("Accept-Encoding").contains("gzip")) {
                    response.putHeader("Content-Encoding", "gzip");
                    byte[] compressedData = gzipCompress(answer);
                    response.setChunked(true);
                    response.write(Buffer.buffer(compressedData));
                    response.end();
                } else {
                    response.end(answer);
                }
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
                .end("{\"error\": \"" + msg + "\"}");
    }

    // Methode zur Gzip-Komprimierung eines Strings
    private byte[] gzipCompress(String data) {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {

            byte[] inputBytes = data.getBytes(StandardCharsets.UTF_8);
            gzipOutputStream.write(inputBytes);
            gzipOutputStream.close();
            return byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            return new byte[0]; // Im Fehlerfall gib ein leeres Array zurück
        }
    }
}
