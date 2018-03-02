package clustering;

/**
 * Interface for a clustermap to combine singletons into groups
 */
public interface IClusterMap {

    /**
     * Adds a link between to singleton ids
     * @param fromId The first singleton
     * @param toId The second singleton
     */
    void addLink(long fromId, long toId);

    /**
     * Gets the current cluster id for a passed singleton id
     * @param singletonId The singleton id
     * @return The corresponding cluster id
     */
    Long get(long singletonId);
}
