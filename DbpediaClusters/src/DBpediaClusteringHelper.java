import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import java.io.*;
import java.util.Scanner;

public class DBpediaClusteringHelper {

    private static boolean isValidUri(String subjectUri) {

        return subjectUri.matches("<http.*>");
    }

    /**
     * Reads a bzipped .nt or .ttl file and writes its content to the cluster map
     * @param singletonMap The singleton map for singleton id lookup
     * @param clusterMap The cluster map to write to
     * @param path The path to the file to read
     * @param lineLimit The max amount of lines to read. Set to -1 for no limit
     */
    public static void readLinkFile(SingletonMap singletonMap, ClusterMap clusterMap, String path, int lineLimit) {

        int linesRead = 0;

        // read data and insert to singleton map
        try {

            FileInputStream fis = new FileInputStream(path);

            Scanner dataScanner = new Scanner(new BZip2CompressorInputStream(fis));

            while(dataScanner.hasNext()) {

                String line = dataScanner.nextLine();
                String[] entries = line.split("\\s");

                if(entries.length > 2) {

                    String subjectUri = entries[0];
                    String objectUri = entries[2];

                    long subjectId = singletonMap.get(subjectUri);
                    long objectId = singletonMap.get(objectUri);

                    clusterMap.addLink(subjectId, objectId);

                    linesRead++;

                    if(linesRead % 100000 == 0) {
                        System.out.println(linesRead + " links read.");
                    }

                    if (lineLimit >= 0) {

                        lineLimit--;

                        if (lineLimit <= 0) {
                            break;
                        }
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Takes a bzipped .nt file and rewrites its content, so that all URIs are replaced with their corresponding
     * DBpedia cluster id
     * @param singletonMap The singleton map to use for singleton id lookup
     * @param clustering The clustering map for cluster id lookup
     * @param path The path of the file to rewrite
     * @param outputSuffix The suffix string to append to the output file
     * @param lineLimit The max amount of lines to read. Set to -1 for no limit
     */
    public static void rewriteDataFiles(SingletonMap singletonMap,
                                        ClusterMap clustering,
                                        String path,
                                        String outputSuffix,
                                        int lineLimit) {

        int linesRead = 0;

        // read data and insert to singleton map
        try {

            // create input and output streams
            FileInputStream fis = new FileInputStream(path);
            String[] splitPath = path.split("\\.", 2);
            FileOutputStream fos = new FileOutputStream(splitPath[0] + outputSuffix + "." + splitPath[1]);

            // create reader and writer
            Scanner dataScanner = new Scanner(new BZip2CompressorInputStream(fis));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos));

            while(dataScanner.hasNext()) {

                // read line per line, do the replacement, write back to file
                String line = dataScanner.nextLine();
                String[] entries = line.split("\\s");

                if(entries.length > 2) {

                    String subjectUri = entries[0];
                    String objectUri = entries[2];

                    line = replaceURI(singletonMap, clustering, line, subjectUri);
                    line = replaceURI(singletonMap, clustering, line, objectUri);

                }

                writer.write(line);
                writer.newLine();

                linesRead++;

                if(linesRead % 100000 == 0) {
                    System.out.println(linesRead + " lines rewritten");
                }

                // line limit check
                if (lineLimit >= 0) {

                    lineLimit--;

                    if (lineLimit <= 0) {
                        break;
                    }
                }
            }

            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static String replaceURI(SingletonMap singletonMap, ClusterMap clustering, String line, String uriToReplace) {

        String result = line;

        if (isValidUri(uriToReplace)) {

            long subjectId = singletonMap.get(uriToReplace);
            Long subjectClusterId = clustering.get(subjectId);

            String subjectReplaceUri = createDBpediaId(subjectClusterId != null ? subjectClusterId : subjectId);
            result = result.replace(uriToReplace, subjectReplaceUri);
        }

        return result;
    }

    private static String createDBpediaId(long subjectClusterId) {

        return "<http://id.dbpedia.org/global/" + Long.toHexString(subjectClusterId) + ">";
    }
}
