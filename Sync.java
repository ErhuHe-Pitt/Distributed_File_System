import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Sync class gets a file from a StorageNode
 * and upload it to another StorageNode.
 * We are using it to get all files from other nodes
 * and upload them to the newly connected node.
 * @author Mesut Erhan Unal and Erhu He
 */
public class Sync extends Thread {
    private JobThread jt;
    private DirectoryServer ds;
    private String node;
    private FileMeta fm;

    /**
     * Default constructor
     * @param _jt JobThread object
     * @param _ds DirectoryServer object
     * @param _node Newly connected node's address
     * @param _fm FileMeta object of the file
     */
    public Sync (JobThread _jt, DirectoryServer _ds, String _node, FileMeta _fm) {
        jt = _jt;
        ds = _ds;
        node = _node;
        fm = _fm;
    }

    @Override
    public void run() {
        byte [] content;

        while (true) {
            String [] source = randomNode().split(":");
            String [] destination = node.split(":");

            try {
                // Open Socket and stream objects to download the file
                Socket sock = new Socket(source[0], Integer.parseInt(source[1]));
                final ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
                final ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());

                // Create request and send
                Message req = new Message("DOWNLOAD");
                req.addContent(fm.getName());
                oos.writeObject(req);

                // Get response
                Message resp = (Message) ois.readObject();
                // Close streams and socket
                oos.close();
                ois.close();
                sock.close();

                // If it is successful, we got the file, read it into content array
                if (resp.getMessage().equals("SUCCESS")) {
                    content = (byte []) resp.getContent().get(1);
                } else {
                    // Node does not have this file. Strange but can be?
                    continue;
                }
            }
            // We could not connect to the StorageNode. Mark it as dead.
            catch (Exception e) {
                ds.setNodeUnavailable(String.format("%s:%s", source[0], source[1]));
                continue;
            }

            try {
                // Open Socket and streams to upload the file onto new node
                Socket sock = new Socket(destination[0], Integer.parseInt(destination[1]));
                final ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
                final ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());

                // Create request and send
                Message req = new Message("UPLOAD");
                req.addContent(fm);
                req.addContent(content);
                oos.writeObject(req);

                // Get response. The body does not matter.
                Message resp = (Message) ois.readObject();

                // Close streams and socket
                oos.close();
                ois.close();
                sock.close();
            }
            // Newly connected node is down. Mark it as dead.
            catch (Exception e) {
                ds.setNodeUnavailable(String.format("%s:%s", destination[0], destination[1]));
                return;
            }

            return;
        }
    }

    /**
     * Returns a random StorageNode to download a file.
     * Uses DirectoryServer.getRandomNode()
     * @return Address of a StorageNode
     */
    public String randomNode() {
        String randomNode = node;
        int i = 0;
        while(randomNode.equals(node)) {
            randomNode = ds.getRandomNode();
            if (++i > 10)
                System.err.println("O-oh. I am the only one here.");
        }
        return randomNode;
    }
}
