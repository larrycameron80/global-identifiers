package clustering;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ciro on 28.02.18.
 */
public class LinkTripleReader implements StreamRDF {

    private final ISingletonMap singletonMap;
    private final IClusterMap clusterMap;
    private final AtomicInteger linesRead;
    private final int lineLimit;

    public LinkTripleReader(ISingletonMap singletonMap, IClusterMap clusterMap, int lineLimit) {

        this.singletonMap = singletonMap;
        this.clusterMap = clusterMap;
        this.lineLimit = lineLimit;

        linesRead = new AtomicInteger(0);
    }

    @Override
    public void start() {

    }

    @Override
    public void triple(Triple triple) {

        Node subject = triple.getSubject();
        Node object = triple.getObject();

        if (subject.isURI() && object.isURI()) {

            int l = linesRead.get();

            if (lineLimit == -1 || l <= lineLimit) {
                long subjectId = singletonMap.get(subject.getURI());
                long objectId = singletonMap.get(object.getURI());

                clusterMap.addLink(subjectId, objectId);

                l = linesRead.incrementAndGet();

                if (l % 100000 == 0) {
                    System.out.println(l + " links read.");
                }
            }
            else {
                Thread.currentThread().stop();
            }
        }
    }

    @Override
    public void quad(Quad quad) {

    }

    @Override
    public void base(String s) {

    }

    @Override
    public void prefix(String s, String s1) {

    }

    @Override
    public void finish() {

    }
}
