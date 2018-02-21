import dbpedia.idmanagement.DBpediaIdClient;
import dbpedia.idmanagement.QueryResponseHandler;

public class ApiTest {

    private static boolean waitForResponse = true;

    public static void main(String[] args) {

        System.out.println("Starting API tests.");

        int errorCount = 0;

        DBpediaIdClient client = new DBpediaIdClient();

        client.DereferenceClusterIriAsync("test", new QueryResponseHandler<String[]>() {
            @Override
            public void OnQueryResponse(String[] response) {

            }

            @Override
            public void OnThrowable(Throwable t) {

            }
        });

        client.GetClusterIriAsync("test", new QueryResponseHandler<String>() {
            @Override
            public void OnQueryResponse(String response) {

            }

            @Override
            public void OnThrowable(Throwable t) {

            }
        });





        while(waitForResponse) {

            System.out.println("Waiting...");
        }

        System.out.printf("Finished API tests with %d errors.", errorCount);
    }
}
