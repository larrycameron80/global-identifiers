package clustering;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.jena.dboe.migrate.L;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;

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
     * @param file The path to the file to read
     * @param lineLimit The max amount of lines to read. Set to -1 for no limit
     */
    public static void readLinkFile(final ISingletonMap singletonMap, final IClusterMap clusterMap, final File file,
                                    final int lineLimit) {

        Thread parsingThread = new Thread() {
            @Override

            public void run() {
                synchronized (this) {

                    try (BZip2CompressorInputStream in = new BZip2CompressorInputStream(new FileInputStream(file))) {

                        Lang language = Lang.TURTLE;

                        if(file.getName().endsWith(".nt.bz")) {
                            language = Lang.NT;
                        }

                        RDFDataMgr.parse(new LinkTripleReader(singletonMap, clusterMap, lineLimit), in, language);

                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    notify();
                }
            }
        };

        parsingThread.start();

        synchronized (parsingThread) {

            try {
                parsingThread.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }

    /**
     * Takes a bzipped .nt file and rewrites its content, so that all URIs are replaced with their corresponding
     * DBpedia cluster id
     * @param singletonMap The singleton map to use for singleton id lookup
     * @param clustering The clustering map for cluster id lookup
     * @param file The file to rewrite
     * @param outputSuffix The suffix string to append to the output file
     * @param lineLimit The max amount of lines to read. Set to -1 for no limit
     */
    public static void rewriteDataFiles(final ISingletonMap singletonMap,
                                        final IClusterMap clustering,
                                        final File file,
                                        String outputPath,
                                        String outputSuffix,
                                        final int lineLimit,
                                        final String[] prefixes) {

        // create the output path
        String fileName = file.getName().substring(0, file.getName().indexOf('.'));
        String path = outputPath + "/" + fileName + outputSuffix + ".nt.bz";



        try(BufferedOutputStream out = new BufferedOutputStream(
                    new BZip2CompressorOutputStream(
                            new FileOutputStream(path)
                )
        )) {

            Thread parsingThread = new Thread() {
                @Override

                public void run() {
                    synchronized (this) {

                        try (BZip2CompressorInputStream in = new BZip2CompressorInputStream(new FileInputStream(file))) {

                            Lang language = Lang.TURTLE;

                            if(file.getName().endsWith(".nt.bz")) {
                                language = Lang.NT;
                            }

                            StreamRDF writer = StreamRDFWriter.getWriterStream(out, language);

                            RDFDataMgr.parse(new DataTripleRewriter(writer, singletonMap, clustering, lineLimit, prefixes), in, language);

                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        notify();
                    }
                }
            };

            parsingThread.start();

            synchronized (parsingThread) {

                try {
                    parsingThread.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
