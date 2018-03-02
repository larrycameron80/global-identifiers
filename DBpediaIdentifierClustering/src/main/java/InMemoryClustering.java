import clustering.DBpediaClusteringHelper;

import java.io.File;
import java.sql.SQLException;

public class InMemoryClustering {

    /**
     * Default data path
     */
    public static String DEFAULT_DATA_PATH = "data";

    /**
     * Default link path
     */
    public static String DEFAULT_LINK_PATH = "links";

    /**
     * Default output path for rewritten data files
     */
    public static String DEFAULT_OUTPUT_PATH = "data/out";

    /**
     * The singleton map file to read
     */
    public static String DEFAULT_SINGLETON_MAP_FILE = "singleton-map.bz2";

    public static void main(String[] args) {

        String dataPath = DEFAULT_DATA_PATH;
        String outputPath = DEFAULT_OUTPUT_PATH;
        String singletonMapFile = DEFAULT_SINGLETON_MAP_FILE;
        String linkPath = DEFAULT_LINK_PATH;
        int limit = -1;

        System.out.println("Starting Clustering...");

        // read command line arguments
        for(int i = 0; i < args.length - 1; i += 2) {

            if(args[i].equals("-d")) {
                dataPath = args[i + 1];
            }

            if(args[i].equals("-l")) {
                linkPath = args[i + 1];
            }

            if(args[i].equals("-o")) {
                outputPath = args[i + 1];
            }

            if(args[i].equals("-s")) {
                singletonMapFile = args[i + 1];
            }

            if(args[i].equals("-limit")) {
                try {
                    limit = Integer.parseInt(args[i + 1]);
                }
                catch(NumberFormatException e) {
                    limit = -1;
                }
            }
        }

        System.out.println("Using data path: " + dataPath);
        System.out.println("Using link path: " + linkPath);
        System.out.println("Using singleton map: " + singletonMapFile);
        System.out.println("Using output path: " + outputPath);
        System.out.println("=============================");

        try {
            Class.forName("virtuoso.jdbc4.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        String connectionString = "jdbc:virtuoso://88.99.242.78:1118/UID=dba/PWD=kerala/log_enable=2";
        // create new maps
        DatabaseSingletonMap singletonMap = new DatabaseSingletonMap(connectionString);
        InMemoryClusterMap clusterMap = new InMemoryClusterMap();

        if(!singletonMap.isConnected()) {

            System.out.println("Singleton Map not connected to databse!");
            return;
        }

        // load singleton map
        System.out.println("Loading singleton map from " + singletonMapFile + "...");

        try {
            singletonMap.load();
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        System.out.println("Done loading singleton map.");

        System.out.println("=============================");

        // get a list of data files that need to be rewritten
        File dataFolder = new File(dataPath);
        File[] dataFiles = dataFolder.listFiles();

        // get a list of sameAs link files for clustering
        File linkFolder = new File(linkPath);
        File[] linkFiles = linkFolder.listFiles();

        // read the link files and create clustering
        for(File file : linkFiles) {

            System.out.println("Reading link file " + file + "...");

            DBpediaClusteringHelper.readLinkFile(singletonMap, clusterMap, file, limit);

            System.out.println("Done reading link file " + file + ".");
        }

        // rewrite data files
        for(File file : dataFiles) {

            if(file.isDirectory()) {
                continue;
            }

            System.out.println("Rewriting data file " + file + "...");

            DBpediaClusteringHelper.rewriteDataFiles(
                    singletonMap,
                    clusterMap,
                    file,
                    outputPath,
                    "-replaced",
                    limit);

            System.out.println("Done rewriting data file " + file + ".");
        }

        // save singleton map
        System.out.println("Saving singleton map to database");

        try {
            singletonMap.save();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        singletonMap.close();

        System.out.println("Done saving singleton map.");

        System.out.println("Clustering finished successfully.");
    }

}
