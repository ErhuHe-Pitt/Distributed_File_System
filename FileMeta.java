import java.io.Serializable;

/**
 * This class is to hold name and size information of a file
 * It needs to be serializable because it will be passed over the network
 * @author Mesut Erhan Unal and Erhu He
 */
public class FileMeta implements Serializable {
    private static final long serialVersionUID = -7226234188134173803L;
    private String name;
    private long size;

    /**
     * Default constructor
     * @param _name File names
     * @param _size File size in bytes
     */
    public FileMeta(String _name, long _size) {
        name = _name;
        size = _size;
    }

    /**
     * File name getter
     * @return File name
     */
    public String getName() {
        return name;
    }

    /**
     * File size getter
     * @return File size in bytes
     */
    public long getSize() {
        return size;
    }
}
