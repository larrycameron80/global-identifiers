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

    public void createTables(String clusteringName) {

        // create tables needed for the clustering // stored procedure already present
    }

    /**
     * Add a link to the clustering, ids already resolved using SingletonMap
     * @param fromId
     * @param toId
     */
    public void addLink(long fromId, long toId) {

        // register the link in the database and save it to name + "_links
        // TODO: Needs a way to tackle duplicates or multiple runs
        // prepare stored procedure etc.
        // CallableStatement addLinkStatement = conn.prepareCall("{call demoSp(?, ?)}");

        // prefer no memory stuff


    }

    public Long get(long id) {

        // TODO: query id from database!
        return _singletonToCluster.get(id);
    }
}
