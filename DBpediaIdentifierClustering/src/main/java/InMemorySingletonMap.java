import clustering.ISingletonMap;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

import java.io.*;
import java.util.HashMap;

public class InMemorySingletonMap implements ISingletonMap {

    /**
     * This variable needs to be tracked globally throughout all datasets to guarantee unique ids
     * (save&load it to some local file)
     */
    private long global_id_counter;

    private HashMap<String, Long> _map;

    public InMemorySingletonMap() {

        _map = new HashMap<String, Long>();
    }

    /**
     * Saves the singleton map to a file
     * @param path
     */
    public void toFile(String path) {

        try {
            // write map to a bzip file
            FileOutputStream fos = new FileOutputStream(path);

            ObjectOutputStream writer = new ObjectOutputStream(new BZip2CompressorOutputStream(fos));

            writer.writeLong(global_id_counter);
            writer.writeObject(_map);

            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Loads the singleton map from a file
     * @param path
     */
    public void fromFile(String path) {

        _map = new HashMap<String, Long>();
        // read map from a bzip file
        try {

            FileInputStream fis = new FileInputStream(path);

            ObjectInputStream reader = new ObjectInputStream(new BZip2CompressorInputStream(fis));

            global_id_counter = reader.readLong();
            _map = (HashMap<String, Long>)reader.readObject();

            reader.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Gets a long identifier from the singleton map
     * @param key
     * @return
     */
    public long get(String key) {

        Long value = _map.get(key);

        if(value == null) {
            value = put(key);
        }

        return value;
    }

    private long put(String key) {

        // insert a new uri, hand out a new long id
        long new_id = global_id_counter++;
        _map.put(key, new_id);

        return new_id;
    }

    public int size() {
        return _map.size();
    }
}
