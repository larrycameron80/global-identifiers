import clustering.ISingletonMap;

import java.sql.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by ciro on 28.02.18.
 */
public class DatabaseRewriteMap implements ISingletonMap {

    private final CallableStatement getIdentifierStatement;

    private final CallableStatement clusteringExistsStatement;

    private Connection conn;

    private ConcurrentHashMap<String, Long> _buffer;

    private final int _maxBufferSize;

    /**
     * Gets a long identifier from the singleton map
     * @param key The lookup URI
     * @return The corresponding long identifier
     */
    public long get(String key) throws SQLException {


        Long result = _buffer.get(key);

        if(result == null) {

            getIdentifierStatement.setString(1, key);
            getIdentifierStatement.execute();

            result = getIdentifierStatement.getLong(2);

            if(_maxBufferSize == -1 || _buffer.size() < _maxBufferSize) {
                _buffer.put(key, result);
            }
        }

        return result;
    }

    /**
     * Database interface to map a URI string to a long identifier. The constructor opens an SQL connection to the database
     * @param connectionString Connection string to the database. Should look like jdbc:virtuoso://YourVirtuosoAddress:1111/UID=YourLogin/PWD=YourPassword/log_enable=2
     * @param clustering The clustering to use. It is already possible to create clusterings with stored procedures.
     * @param maxBufferSize Creates a local cache. MaxBufferSize indicates the maximum size of the buffer. -1 For no limit
     * @throws SQLException
     */
    public DatabaseRewriteMap(String connectionString, String clustering, int maxBufferSize) throws SQLException {


        // creates the buffer
        _maxBufferSize = maxBufferSize;
        _buffer = new ConcurrentHashMap<>();

        // creates the sql connection and statements for lookup
        conn = DriverManager.getConnection(connectionString);
        clusteringExistsStatement = conn.prepareCall("{call DBpediaClusteringExists(?, ?)}");

        if(clustering != null) {

            clusteringExistsStatement.registerOutParameter(2, Types.INTEGER);
            clusteringExistsStatement.setString(1, clustering);
            clusteringExistsStatement.execute();

            if(clusteringExistsStatement.getInt(2) == 0) {
                System.err.println("Clustering " + clustering + " does not exist in the database. Proceeding without clustering information.");
                getIdentifierStatement = conn.prepareCall("{call DBpediaGetIdentifier(?, ?)}");
                getIdentifierStatement.registerOutParameter(2, Types.BIGINT);
            } else {
                getIdentifierStatement = conn.prepareCall("{call DBpediaGetIdentifierInClustering(?, ?, ?)}");
                getIdentifierStatement.registerOutParameter(3, Types.BIGINT);
                getIdentifierStatement.setString(2, clustering);
            }
        }
        else
        {
            getIdentifierStatement = conn.prepareCall("{call DBpediaGetIdentifier(?, ?)}");
            getIdentifierStatement.registerOutParameter(2, Types.BIGINT);
        }
    }

    /**
     * Closes the SQL connection
     */
    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}