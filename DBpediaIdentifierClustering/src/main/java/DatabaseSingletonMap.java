import clustering.ISingletonMap;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ciro on 28.02.18.
 */
public class DatabaseSingletonMap implements ISingletonMap {


    private Connection conn;
    private long global_id_counter;

    private HashMap<String, Long> _map;

    private HashMap<String, Long> _mapNewEntries;

    private boolean isConnected;

    /**
     * Gets a long identifier from the singleton map
     * @param key
     * @return
     */
    public long get(String key) {

        Long value = _map.get(key);

        if(value == null) {

            value = _mapNewEntries.get(key);

            if(value == null) {
                value = put(key);
            }
        }

        return value;
    }

    private long put(String key) {

        // insert a new uri, hand out a new long id
        long new_id = global_id_counter++;
        _mapNewEntries.put(key, new_id);

        return new_id;
    }

    public DatabaseSingletonMap(String connectionString) {

        _map = new HashMap<String, Long>();
        _mapNewEntries = new HashMap<String, Long>();

        try {
            conn = DriverManager.getConnection(connectionString);
            isConnected = true;

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean isConnected()
    {
        return isConnected;
    }


    public void load() throws SQLException {

        // load id counter from database
        Statement counterStmt = conn.createStatement();
        String counterSql = "SELECT Counter FROM DBpediaIdCounter";

        ResultSet rs = counterStmt.executeQuery(counterSql);

        while(rs.next()){
            //Retrieve by column name
            global_id_counter = rs.getLong("Counter");
        }

        rs.close();

        // load hashmap from database
        Statement stmt = conn.createStatement();

        String sql = "SELECT * FROM DBpediaSingletonMap";
        rs = stmt.executeQuery(sql);

        while(rs.next()){
            //Retrieve by column name
            long singletonId  = rs.getLong("SingletonId");
            String iri = rs.getString("DbpediaId");

            _map.put(iri, singletonId);
        }

        rs.close();



    }

    public void save() throws SQLException {

        PreparedStatement ps = conn.prepareStatement("INSERT INTO DBpediaSingletonMap VALUES (?, ?)");

        for(Map.Entry<String, Long> entry : _mapNewEntries.entrySet()) {

            ps.clearParameters();
            ps.setString(1, entry.getKey());
            ps.setLong(2, entry.getValue());
            ps.addBatch();
        }

        ps.clearParameters();
        ps.executeBatch();

        Statement stmt = conn.createStatement();

        String sql = "UPDATE DBpediaIdCounter SET Counter = "+global_id_counter;
        stmt.executeUpdate(sql);

    }

}