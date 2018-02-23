import java.util.ArrayList;

public class DBpediaClustering {

    public static void main(String[] args) {

        // create new maps
        SingletonMap singletonMap = new SingletonMap();
        ClusterMap clusterMap = new ClusterMap();

        // load singleton map
        System.out.println("Loading singleton map.");
        singletonMap.fromFile("singleton-map.bz2");
        System.out.println("Done.");

        // get a list of data files that need to be rewritten
        ArrayList<String> dataFiles = new ArrayList<>();

        dataFiles.add("data/permid-orgs.nt.bz2");
        dataFiles.add("data/geonames.nt.bz2");

        // get a list of sameAs link files for clustering
        ArrayList<String> linkFiles = new ArrayList<>();
        linkFiles.add("data/wikidata-sameas-permid.nt.bz2");
        linkFiles.add("data/wikidata-sameas-external.ttl.bz2");

        // read the link files and create clustering
        for(String file : linkFiles) {
            System.out.println("Reading link file " + file);
            DBpediaClusteringHelper.readLinkFile(singletonMap, clusterMap, file, -1);
            System.out.println("Done reading link file " + file);
        }

        // rewrite data files
        for(String file : dataFiles) {
            System.out.println("Rewriting data file " + file);
            DBpediaClusteringHelper.rewriteDataFiles(singletonMap, clusterMap, file, "_replaced", -1);
            System.out.println("Done rewriting data file " + file);
        }

        // save singleton map
        System.out.println("Saving singleton map.");
        singletonMap.toFile("singleton-map.bz2");
        System.out.println("Done.");
    }

}
