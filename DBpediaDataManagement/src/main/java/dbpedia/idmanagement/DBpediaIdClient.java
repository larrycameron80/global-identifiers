package dbpedia.idmanagement;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import sun.net.www.http.HttpClient;

import java.util.concurrent.Semaphore;

public class DBpediaIdClient {

    /**
     * Will return the Cluster IRI (DBpedia-ID) of the passed IRI
     * @param queryIRI
     * @param responseHandler
     * @return
     */
    public void GetClusterIriAsync(String queryIRI, final QueryResponseHandler<String> responseHandler) {

        // TODO: Generate the query url from the clusterIRI param
        String queryUrl = "";

        AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
        asyncHttpClient.prepareGet(queryUrl).execute(new AsyncCompletionHandler<Response>(){

            @Override
            public Response onCompleted(Response response) {

                // TODO: fetch the result from the response param
                String result = null;

                responseHandler.OnQueryResponse(result);
                return response;
            }

            @Override
            public void onThrowable(Throwable t){
                responseHandler.OnThrowable(t);
            }
        });
    }

    public String[] DereferenceClusterIri(String clusterIRI) {

        final Semaphore semaphore = new Semaphore(0);

        DereferenceClusterIriAsync(clusterIRI, new QueryResponseHandler<String[]>() {
            @Override
            public void OnQueryResponse(String[] response) {

                semaphore.release();
            }

            @Override
            public void OnThrowable(Throwable t) {

            }
        });

        semaphore.acquireUninterruptibly();
    }

    /**
     * Find all IRI contained in the cluster with the given IRI
     * @param clusterIRI
     * @param responseHandler
     * @return
     */
    public void DereferenceClusterIriAsync(String clusterIRI, final QueryResponseHandler<String[]> responseHandler) {

        // TODO: Generate the query url from the clusterIRI param
        String queryUrl = "";

        AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
        asyncHttpClient.prepareGet(queryUrl).execute(new AsyncCompletionHandler<Response>(){

            @Override
            public Response onCompleted(Response response) {

                // TODO: fetch the result from the response param
                String[] result = null;

                responseHandler.OnQueryResponse(result);
                return response;
            }

            @Override
            public void onThrowable(Throwable t){
                responseHandler.OnThrowable(t);
            }
        });
    }

    public static void ValidateClusterIRI() {


    }

}
