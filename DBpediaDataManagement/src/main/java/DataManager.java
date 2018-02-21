public class DataManager {

    public static final int SUCCESS = 0;

    public static final int ERROR_CONNECTION_FAILED = 1;

    public static final int ERROR_TURTLE_PARSE_ERROR = 2;

    public DataManager()
    {

    }

    public int Connect(String connectionString)
    {
        return ERROR_CONNECTION_FAILED;
    }

    public int ParseTurtle(String path)
    {
        return ERROR_TURTLE_PARSE_ERROR;
    }

    public int InsertLink(String subject, String predicate, String Object)
    {
        return -1;
    }

    public int InsertUri(String uri)
    {
        return -1;
    }
}
