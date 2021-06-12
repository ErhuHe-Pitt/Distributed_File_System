import java.io.Serializable;
import java.util.ArrayList;

/**
 * This class is the message envelope that will be passed
 * over the network for machine-to-machine communication
 * @author Mesut Erhan Unal and Erhu He
 */
public class Message implements Serializable {
    private static final long serialVersionUID = -7226234188134173803L;
    private String message;
    private ArrayList<Object> content;

    /**
     * Default constructor
     * Instantiates a new object with message attribute
     * and creates and empty content list
     * @param _message: Header of the message
     */
    public Message(String _message) {
        message = _message;
        content = new ArrayList<Object>();
    }

    /**
     * Adds an object to the content list
     * It can be a file list sent from peer to indexing server while registering
     * or it can be specified chunk of a file requested from a peer
     * @param o: Content to be added to the message envelope
     */
    public void addContent(Object _o) {
        content.add(_o);
    }

    /**
     * Message header getter
     * @return message header
     */
    public String getMessage() {
        return message;
    }

    /**
     * Content list getter
     * @return content list
     */
    public ArrayList<Object> getContent() {
        return content;
    }
}
