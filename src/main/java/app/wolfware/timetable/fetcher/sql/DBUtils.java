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

    private final static String select_train = "SELECT t.id, t.number, j.position, s.name, s.alias, j.arrival_pt, j.arrival_pp, j.departure_pt, j.departure_pp, art.number AS 'arrival_number', art.id AS 'arrival_id', dt.number AS 'departure_number', dt.id AS 'departure_id' FROM train t left join journey j on j.id = t.id LEFT JOIN station s ON s.id = j.station LEFT JOIN train art ON art.id = j.arrival_wings LEFT JOIN train dt ON dt.id = j.departure_wings WHERE t.number = ? AND t.id like ? ORDER BY j.position ASC";

    private final static DateTimeFormatter formatterDateTime = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm");

    public static String selectTrains(int train, String date) {

        Map<String, Object> resultMap = new HashMap<>();
        List<Map<String, Object>> stations = new ArrayList<>();

        boolean foundTrain = false;

        try (Connection conn = DBUtils.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(select_train)) {
            pstmt.setInt(1, train);
            pstmt.setString(2, "%-" + date + "%");

            String id = null;
            int number = -1;

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                if (number == -1) {
                    number = rs.getInt("number");
                    id = rs.getString("id");
                    foundTrain = true;
                }
                Map<String, Object> stationData = new HashMap<>();

                stationData.put("position", rs.getInt("position"));
                stationData.put("station", rs.getString("name"));
                stationData.put("station_alias", rs.getString("alias"));

                Timestamp arrivalTimestamp = rs.getTimestamp("arrival_pt");
                Timestamp departureTimestamp = rs.getTimestamp("departure_pt");

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
                stationData.put("arrival_pp", rs.getString("arrival_pp"));
                stationData.put("departure_pp", rs.getString("departure_pp"));
                //stationData.put("wings_arrival_id", rs.getString("arrival_id"));
                stationData.put("wings_arrival_number", rs.getString("arrival_number"));
                //stationData.put("wings_departure_id", rs.getString("departure_id"));
                stationData.put("wings_departure_number", rs.getString("departure_number"));
                stations.add(stationData);
            }
            resultMap.put("stations", stations);
            resultMap.put("number", number);
            resultMap.put("id", id);
        } catch (SQLException e) {
            e.printStackTrace();
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
