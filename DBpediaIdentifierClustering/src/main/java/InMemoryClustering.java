import clustering.DBpediaClusteringHelper;

import java.io.File;
import java.nio.file.Files;
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
        String[] prefixes = null;
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

            if(args[i].equals("-prefix")) {
                prefixes = args[i + 1].split(",");
            }
        }

        System.out.println("Using data path: " + dataPath);
        System.out.println("Using link path: " + linkPath);
        System.out.println("Using singleton map: " + singletonMapFile);
        System.out.println("Using output path: " + outputPath);
        System.out.println("=============================");

        findOrCreatePath(dataPath);
        findOrCreatePath(outputPath);
        findOrCreatePath(linkPath);

        try {
            Class.forName("virtuoso.jdbc4.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        String connectionString = "jdbc:virtuoso://127.0.0.1:1111/UID=dba/PWD=dba/log_enable=2";
        // create new maps
        DatabaseSingletonMap singletonMap;

        try {
            singletonMap = new DatabaseSingletonMap(connectionString, 1000000);
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        InMemoryClusterMap clusterMap = new InMemoryClusterMap();


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
                    limit,
                    prefixes);

            System.out.println("Done rewriting data file " + file + ".");
        }



        singletonMap.close();


        System.out.println("Clustering finished successfully.");
    }

    private static void findOrCreatePath(String path) {
        File folder = new File(path);
        if(!folder.exists() || !folder.isDirectory()) {
            folder.mkdir();
        }

    }

}
