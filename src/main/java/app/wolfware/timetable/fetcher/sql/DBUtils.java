package app.wolfware.timetable.fetcher.sql;

import app.wolfware.timetable.fetcher.security.Credentials;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBUtils {

    private final static String select_train = "SELECT t.id, t.number, t.origin, t.destination, j.position, s.name, s.alias, j.arrival_pt, j.arrival_pp, j.departure_pt, j.departure_pp, jc.arrival_ct, jc.arrival_cp, jc.departure_ct, jc.departure_cp, jc.arrival_cs, jc.departure_cs, jc.arrival_clt, jc.departure_clt, art.number AS 'arrival_number', art.id AS 'arrival_id', dt.number AS 'departure_number', dt.id AS 'departure_id', tt.id AS 'transition_id', tt.number AS 'transition_number', tt.timestamp AS 'transition_timestamp', tt.origin AS 'transition_origin', tt.destination AS 'transition_destination' FROM train t left join journey j on j.id = t.id LEFT JOIN station s ON s.id = j.station LEFT JOIN train art ON art.id = j.arrival_wings LEFT JOIN train dt ON dt.id = j.departure_wings LEFT JOIN train tt on tt.id = j.transition LEFT JOIN journey_changes jc ON j.id = jc.id AND jc.position = j.position WHERE t.number = ? AND t.id like ? ORDER BY j.position ASC;";
    private final static String select_train_and_find_date = "WITH searched_train AS (SELECT t.id, t.number FROM train t WHERE t.number = ? AND t.timestamp <= NOW() + INTERVAL 16 HOUR ORDER BY t.timestamp DESC LIMIT 1) SELECT t.id, t.number FROM train t LEFT JOIN searched_train st ON st.id = t.id WHERE t.id = st.id ORDER BY t.timestamp DESC LIMIT 1;";
    private final static String select_additional_info = "SELECT * from additional_train_info WHERE id = ?";
    private final static DateTimeFormatter formatterDateTime = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm");
    private static final Map<String, String> METADATA;

    static {
        METADATA = new HashMap<>();
        METADATA.put("source", "Deutsche Bahn Timetables API");
        METADATA.put("license", "CC BY 4.0");
        METADATA.put("license_url", "https://creativecommons.org/licenses/by/4.0/");
        METADATA.put("api_provider", "https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables");
        METADATA.put("changes", "The data structure has been modified, but the underlying data comes directly from the Deutsche Bahn Timetables API.");
    }

    public static String selectTrains(int train, String date, String meta) {
        if (date == null) {
            try (Connection conn = DBUtils.getConnection();
                 PreparedStatement pstmt = conn.prepareStatement(select_train_and_find_date)) {
                pstmt.setInt(1, train);
                return selectTrains(pstmt.executeQuery(), meta);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            try (Connection conn = DBUtils.getConnection();
                 PreparedStatement pstmt = conn.prepareStatement(select_train)) {
                pstmt.setInt(1, train);
                pstmt.setString(2, "%-" + date + "%");
                return selectTrains(pstmt.executeQuery(), meta);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private static String selectTrains(ResultSet rs, String meta) throws SQLException {

        Map<String, Object> resultMap = new HashMap<>();
        List<Map<String, Object>> stations = new ArrayList<>();

        boolean foundTrain = false;
        String id = null;


        int number = -1;
        String origin = null;
        String destination = null;

        while (rs.next()) {
            if (number == -1) {
                number = rs.getInt("number");
                id = rs.getString("id");
                origin = rs.getString("origin");
                destination = rs.getString("destination");
                foundTrain = true;
            }
            Map<String, Object> stationData = new HashMap<>();

            stationData.put("position", rs.getInt("position"));
            stationData.put("station", rs.getString("name"));
            stationData.put("station_alias", rs.getString("alias"));

            Timestamp arrivalTimestamp = rs.getTimestamp("arrival_pt");
            Timestamp departureTimestamp = rs.getTimestamp("departure_pt");
            Timestamp changeArrivalTimestamp = rs.getTimestamp("arrival_ct");
            Timestamp changeDepartureTimestamp = rs.getTimestamp("departure_ct");

            if (arrivalTimestamp != null) {
                stationData.put("arrival_pt", formatterDateTime.format(arrivalTimestamp.toLocalDateTime()));
            } else {
                stationData.put("arrival_pt", null);
            }
            if (departureTimestamp != null) {
                stationData.put("departure_pt", formatterDateTime.format(departureTimestamp.toLocalDateTime()));
            } else {
                stationData.put("departure_pt", null);
            }
            if (changeArrivalTimestamp != null) {
                stationData.put("arrival_ct", formatterDateTime.format(changeArrivalTimestamp.toLocalDateTime()));
            }
            if (changeDepartureTimestamp != null) {
                stationData.put("departure_ct", formatterDateTime.format(changeDepartureTimestamp.toLocalDateTime()));
            }
            stationData.put("arrival_pp", rs.getString("arrival_pp"));
            stationData.put("departure_pp", rs.getString("departure_pp"));
            if (rs.getString("arrival_cp") != null) {
                stationData.put("arrival_cp", rs.getString("arrival_cp"));
            }
            if (rs.getString("departure_cp") != null) {
                stationData.put("departure_cp", rs.getString("departure_cp"));
            }
            String arrival_cs = rs.getString("arrival_cs");
            String departure_cs = rs.getString("departure_cs");
            Timestamp arrival_clt = rs.getTimestamp("arrival_clt");
            Timestamp departure_clt = rs.getTimestamp("departure_clt");
            if (arrival_cs != null) {
                stationData.put("arrival_cs", arrival_cs);
                if (arrival_cs.equals("c") && arrival_clt != null) {
                    stationData.put("arrival_clt", formatterDateTime.format(arrival_clt.toLocalDateTime()));
                }
            }
            if (departure_cs != null) {
                stationData.put("departure_cs", departure_cs);
                if (departure_cs.equals("c") && departure_clt != null) {
                    stationData.put("departure_clt", formatterDateTime.format(departure_clt.toLocalDateTime()));
                }
            }
            //stationData.put("wings_arrival_id", rs.getString("arrival_id"));
            stationData.put("wings_arrival_number", rs.getString("arrival_number"));
            //stationData.put("wings_departure_id", rs.getString("departure_id"));
            stationData.put("wings_departure_number", rs.getString("departure_number"));
            String transitionId = rs.getString("transition_id");
            String transitionNumber = rs.getString("transition_number");
            Timestamp transitionTimestamp = rs.getTimestamp("transition_timestamp");
            String transitionOrigin = rs.getString("transition_origin");
            String transitionDestination = rs.getString("transition_destination");
            if (transitionId != null) {
                Map<String, String> transitionData = new HashMap<>();
                transitionData.put("id", transitionId);
                transitionData.put("number", transitionNumber);
                transitionData.put("timestamp", formatterDateTime.format(transitionTimestamp.toLocalDateTime()));
                transitionData.put("origin", transitionOrigin);
                transitionData.put("destination", transitionDestination);
                stationData.put("transition", transitionData);
            }
            stations.add(stationData);
        }
        resultMap.put("stations", stations);
        resultMap.put("origin", origin);
        resultMap.put("destination", destination);
        resultMap.put("number", number);
        resultMap.put("id", id);
        if (meta == null || !meta.equals("0")) {
            resultMap.put("metadata", METADATA);
        }


        if (foundTrain) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                return objectMapper.writeValueAsString(resultMap);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(Credentials.URL, Credentials.USER, Credentials.PASSWORD);
    }
}
