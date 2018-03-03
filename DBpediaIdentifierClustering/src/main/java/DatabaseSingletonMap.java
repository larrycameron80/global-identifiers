import clustering.ISingletonMap;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by ciro on 28.02.18.
 */
public class DatabaseSingletonMap implements ISingletonMap {

    private final CallableStatement cStmt;

    private Connection conn;

    private ConcurrentHashMap<String, Long> _buffer;

    private final int _maxBufferSize;

    /**
     * Gets a long identifier from the singleton map
     * @param key
     * @return
     */
    public long get(String key) throws SQLException {


        Long result = _buffer.get(key);

        if(result == null) {


            cStmt.setString(1, key);

            cStmt.execute();

            result = cStmt.getLong(2);

            if(_buffer.size() < _maxBufferSize) {
                _buffer.put(key, result);
            }
        }

        return result;
    }

    public DatabaseSingletonMap(String connectionString, int maxBuferSize) throws SQLException {

        _maxBufferSize = maxBuferSize;
        _buffer = new ConcurrentHashMap<>();

        conn = DriverManager.getConnection(connectionString);

        cStmt = conn.prepareCall("{call DBpediaGetIdentifier(?, ?)}");
        cStmt.registerOutParameter(2, Types.BIGINT);

    }

    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}