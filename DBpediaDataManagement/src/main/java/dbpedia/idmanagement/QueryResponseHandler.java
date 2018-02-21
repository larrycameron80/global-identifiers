package dbpedia.idmanagement;

public abstract class QueryResponseHandler<T> {

    public abstract void OnQueryResponse(T response);

    public abstract void OnThrowable(Throwable t);
}
