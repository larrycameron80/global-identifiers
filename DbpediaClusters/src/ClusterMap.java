import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ClusterMap {

    private HashMap<Long, ArrayList<Long>> _clusterToSingletons;

    private HashMap<Long, Long> _singletonToCluster;

    public  ClusterMap() {
        _clusterToSingletons = new HashMap<>();
        _singletonToCluster = new HashMap<>();
    }

    /**
     * Add a link to the clustering, ids already resolved using SingletonMap
     * @param fromId
     * @param toId
     */
    public void addLink(long fromId, long toId) {

        // get the current cluster of the from and to id
        Long clusterFrom = _singletonToCluster.get(fromId);
        Long clusterTo = _singletonToCluster.get(toId);

        // if there is no entry yet, create one
        if(clusterFrom == null) {
            _singletonToCluster.put(fromId, fromId);
            clusterFrom = fromId;
        }

        // if there is no entry yet, create one
        if(clusterTo == null) {
            _singletonToCluster.put(toId, toId);
            clusterTo = toId;
        }

        // the clusters differ
        if(clusterFrom != clusterTo) {

            // target cluster id is the smallest of the two cluster ids
            long minor = Math.min(clusterFrom, clusterTo);
            long major = Math.max(clusterFrom, clusterTo);

            _singletonToCluster.put(fromId, minor);
            _singletonToCluster.put(toId, minor);

            // no entry for minor cluster yet, create new list and add self
            if(!_clusterToSingletons.containsKey(minor)) {
                _clusterToSingletons.put(minor, new ArrayList<>());
                _clusterToSingletons.get(minor).add(minor);
            }

            // already entry for major, move all entries over to minor
            if(_clusterToSingletons.containsKey(major)) {

                _clusterToSingletons.get(minor).addAll(_clusterToSingletons.get(major));
                _clusterToSingletons.remove(major);

            }
            // no entry for major yet, just add to minor
            else {
                _clusterToSingletons.get(minor).add(major);
            }
        }
    }

    public Long get(long id) {

        return _singletonToCluster.get(id);
    }
}
