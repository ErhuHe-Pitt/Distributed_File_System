import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Uploader class is to upload a file onto a StorageNode.
 * It is used to send a new file to all other StorageNodes
 * in the system.
 * @author Mesut Erhan Unal and Erhu He
 */
public class Uploader extends Thread {
    private JobThread jt;
    private DirectoryServer ds;
    private String node;
    private FileMeta fm;
    private byte [] content;

    /**
     * Default constructor
     * @param _jt JobThread object
     * @param _ds DirectoryServer object
     * @param _node StorageNode address to upload the file onto
     * @param _fm FileMeta object of the file
     * @param _content Content of the file
     */
    public Uploader (JobThread _jt, DirectoryServer _ds, String _node, FileMeta _fm, byte [] _content) {
        jt = _jt;
        ds = _ds;
        node = _node;
        fm = _fm;
        content = _content;
    }

    @Override
    public void run () {
        try {
            String [] addr = node.split(":");
            // Open socket and streams to upload the file onto a StorageNode
            Socket sock = new Socket(addr[0], Integer.parseInt(addr[1]));
            final ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
            final ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());

            // Create request and send it
            Message req = new Message("UPLOAD");
            req.addContent(fm);
            req.addContent(content);
            oos.writeObject(req);

            // Get response but body does not matter
            Message resp = (Message) ois.readObject();

            // Close streams and socket
            oos.close();
            ois.close();
            sock.close();
        }
        // StorageNode is unavailable, mark it as dead.
        catch (Exception e) {
            ds.setNodeUnavailable(node);
        }
    }
}
