import clustering.DBpediaClusteringHelper;

import java.io.File;
import java.sql.SQLException;

public class DataRewriter {

    /**
     * Default data path
     */
    public static String DEFAULT_DATA_PATH = "data";

    /**
     * Default link path
     */
    public static String DEFAULT_CONN_STRING = "jdbc:virtuoso://127.0.0.1:1111/UID=dba/PWD=dba/log_enable=2";

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
        String connectionString = DEFAULT_CONN_STRING;
        String[] prefixes = null;
        int limit = -1;


        // read command line arguments
        for(int i = 0; i < args.length - 1; i += 2) {

            if(args[i].equals("-d")) {
                dataPath = args[i + 1];
            }

            if(args[i].equals("-o")) {
                outputPath = args[i + 1];
            }

            if(args[i].equals("-c")) {
                connectionString = args[i + 1];
            }

            if(args[i].equals("-l")) {
                try {
                    limit = Integer.parseInt(args[i + 1]);
                }
                catch(NumberFormatException e) {
                    limit = -1;
                }
            }

            if(args[i].equals("-n")) {
                prefixes = args[i + 1].split(",");
            }
        }

        System.out.println("Starting Rewriting with the following parameters:");
        System.out.println("=============================");
        System.out.println("Data path: " + dataPath);
        System.out.println("Output path: " + outputPath);
        System.out.println("Connection string: " + connectionString);
        System.out.println("Line limit: " + ((limit == -1) ? "Off" : limit));
        System.out.print("Namespace restrictions: ");

        if(prefixes == null) {
            System.out.println("Off");
        } else {
            for(String prefix : prefixes) {
                System.out.print(prefix + ";");
            }
            System.out.println();
        }

        System.out.println("=============================");

        findOrCreatePath(dataPath);
        findOrCreatePath(outputPath);

        try {
            Class.forName("virtuoso.jdbc4.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        // create new maps
        DatabaseSingletonMap singletonMap;

        try {
            singletonMap = new DatabaseSingletonMap(connectionString, 1000000);
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        InMemoryClusterMap clusterMap = new InMemoryClusterMap();



        // get a list of data files that need to be rewritten
        File dataFolder = new File(dataPath);
        File[] dataFiles = dataFolder.listFiles();

        // get a list of sameAs link files for clustering

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
            System.out.println("=============================");

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
