package clustering;

import org.apache.jena.graph.*;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ciro on 28.02.18.
 */
public class DataTripleRewriter implements StreamRDF {

    private final ISingletonMap singletonMap;
    private final IClusterMap clusterMap;
    private final AtomicInteger linesRead;
    private final int lineLimit;
    private final StreamRDF writer;
    private final String[] prefixes;

    public DataTripleRewriter(StreamRDF writer, ISingletonMap singletonMap, IClusterMap clusterMap, int lineLimit,
                              String[] prefixes) {

        this.prefixes = prefixes;
        this.singletonMap = singletonMap;
        this.clusterMap = clusterMap;
        this.lineLimit = lineLimit;
        this.writer = writer;

        linesRead = new AtomicInteger(0);
    }

    @Override
    public void start() {
        writer.start();
    }

    @Override
    public void triple(Triple triple) {

        Node subject = triple.getSubject();
        Node object = triple.getObject();
        Node predicate = triple.getPredicate();

        int l = linesRead.get();

        if (lineLimit == -1 || l <= lineLimit) {


            subject = replaceNode(subject);
            object = replaceNode(object);

            writer.triple(new Triple(subject, predicate, object));

            l = linesRead.incrementAndGet();

            if (l % 100000 == 0) {
                System.out.println(l + " links read.");
            }

        } else {
            Thread.currentThread().stop();
        }
    }

    private Node replaceNode(Node object) {

        if (object.isURI()) {

            String uri = object.getURI();

            if(prefixes != null) {
                boolean hasMatch = false;

                for(String prefix : prefixes) {

                    if(uri.endsWith(prefix)) {
                        hasMatch = true;
                        break;
                    }
                }

                if(!hasMatch) {
                    return object;
                }
            }

            long objectId = 0;

            try {
                objectId = singletonMap.get(object.getURI());
            } catch (SQLException e) {
                e.printStackTrace();
                return object;
            }

            Long objectClusterId = clusterMap.get(objectId);

            String objectReplaceUri = createDBpediaId(objectClusterId != null ? objectClusterId : objectId);

            return NodeFactory.createURI(objectReplaceUri);
        }

        return object;
    }

    @Override
    public void quad(Quad quad) {
        triple(quad.asTriple());
    }

    @Override
    public void base(String s) {
        writer.base(s);
    }

    @Override
    public void prefix(String s, String s1) {
        writer.prefix(s, s1);
    }

    @Override
    public void finish() {
        writer.finish();
    }


    private String createDBpediaId(long subjectClusterId) {
        return "http://id.dbpedia.org/global/" + Long.toHexString(subjectClusterId);
    }

}
