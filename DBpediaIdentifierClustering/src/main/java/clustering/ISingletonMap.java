package clustering;

import java.sql.SQLException;

public interface ISingletonMap {

    /**
     * Gets a unique singleton id for a string identifier
     * @param iri The string identifier
     * @return The singleton identifier as long integer
     */
    long get(String iri) throws SQLException;

}
