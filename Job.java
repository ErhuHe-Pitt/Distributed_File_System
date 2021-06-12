import java.io.Serializable;
import java.util.ArrayList;

/**
 * Job class is a basic class to hold a DirectoryServer job information.
 * Since job queue move over the network, this class should be serializable.
 * @author Mesut Erhan Unal and Erhu He
 */
public class Job implements Serializable {
    private static final long serialVersionUID = -7226234188134173803L;
    private String type;
    private String node;
    private ArrayList<Object> content;

    /**
     * Default constructor
     * @param _type Type of the job
     * @param _node Source of the job
     */
    public Job(String _type, String _node) {
        type = _type;
        node = _node;
        content = new ArrayList<Object>();
    }

    /**
     * Job type getter
     * @return Job type
     */
    public String getType() {
        return type;
    }

    /**
     * Node getter
     * @return Node
     */
    public String getNode() {
        return node;
    }

    /**
     * Adds content into Job object
     * @param _o Object to be added
     */
    public void addContent(Object _o) {
        content.add(_o);
    }

    /**
     * Gets content list
     * @return Content list
     */
    public ArrayList<Object> getContent() {
        return content;
    }
}
