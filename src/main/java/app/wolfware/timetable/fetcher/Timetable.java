package app.wolfware.timetable.fetcher;

import app.wolfware.timetable.fetcher.api.TimetableFetcherAPI;
import io.vertx.core.Vertx;


public class Timetable {

    public static void main(String[] args) {

        TimetableFetcherAPI api = new TimetableFetcherAPI();

        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(api);
    }

}
