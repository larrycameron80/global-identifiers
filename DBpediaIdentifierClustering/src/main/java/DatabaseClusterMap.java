import clustering.IClusterMap;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class DatabaseClusterMap implements IClusterMap {

    private final String name;
    private boolean isConnected;

    private Connection conn;

    private HashMap<Long, ArrayList<Long>> _clusterToSingletons;

    private HashMap<Long, Long> _singletonToCluster;

    public DatabaseClusterMap(String name, String connectionString) {

        this.name = name;

        if(name == null)
            return;

        try {
            conn = DriverManager.getConnection(connectionString);
            isConnected = true;

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public boolean isConnected()
    {
        return isConnected;
    }

    public void createTables(String clusteringName)

    /**
     * Add a link to the clustering, ids already resolved using SingletonMap
     * @param fromId
     * @param toId
     */
    public void addLink(long fromId, long toId) {

        CallableStatement addLinkStatement = conn.prepareCall("{call demoSp(?, ?)}");
    }

    public Long get(long id) {

        return _singletonToCluster.get(id);
    }
}
