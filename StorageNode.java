import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * StorageNode class listens a specific port and creates a
 * StorageNodeThread for each incoming connection.
 * @author Mesut Erhan Unal and Erhu He
 */
public class StorageNode extends Server {
    private Address [] dirServers;
    private AtomicInteger dirServerIndex;
    private boolean test;

    /**
     * Default constructor
     * @param _port Port number to listen
     * @param _serverID Unique node ID
     * @param _dirIP Primary DirectoryServer's IP
     * @param _dirPort Primary DirectoryServer's port
     * @param _backupIP Backup DirectoryServer's IP
     * @param _backupPort Backup DirectoryServer's port
     * @param _test Test flag
     */
    public StorageNode(int _port, String _serverID, String _dirIP, int _dirPort, String _backupIP, int _backupPort, boolean _test) {
        super(_port, _serverID);
        dirServers = new Address [] {new Address(_dirIP, _dirPort), new Address(_backupIP, _backupPort)};
        dirServerIndex = new AtomicInteger(0);
        test = _test;
    }

    @Override
    public void start() {
        try {
            if (test) fillMap();

            // Add a shutdown hook
            Runtime runtime = Runtime.getRuntime();
            runtime.addShutdownHook(new ServerShutDownListener(this));

            // Check if there is a directory for this node. Create one if it does not already exist.
            File dir = new File("./" + serverID);
            if (!dir.isDirectory()) dir.mkdir();

            // Register with the DirectoryServer
            register();

            // Create a ServerSocket and start listening the port
            final ServerSocket serverSock = new ServerSocket(port);
            System.out.println(String.format("StorageNode is running on %s:%d", Utils.getIP(), port));

            Socket sock = null;
            StorageNodeThread t = null;

            while (true) {
                sock = serverSock.accept();
                t = new StorageNodeThread(sock, this);
                t.start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Registers to the DirectoryServer
     */
    public void register() {
        try {
            // Open socket and streams to DirectoryServer
            Socket sock = new Socket(dirServers[dirServerIndex.get()].getIP(), dirServers[dirServerIndex.get()].getPort());
            final ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
            final ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());

            // Create message and send
            Message req = new Message("REGISTER");
            req.addContent(String.format("%s:%d", Utils.getIP(), port));
            oos.writeObject(req);

            // Get response. Body does not matter.
            Message resp = (Message) ois.readObject();

            // Close streams and socket.
            oos.close();
            ois.close();
            sock.close();
        }
        // DirectoryServer is down. Switch to the backup.
        catch (Exception e) {
            switchServer();
            register();
        }
    }

    /**
     * Switches DirectoryServer to use from primary to backup.
     * Kills the store node if both DirectoryServers are down.
     */
    public void switchServer() {
        if (!dirServerIndex.compareAndSet(0, 1)) System.exit(0);
    }

    /**
     * Sends new file to the DirectoryServer
     * @param _fm FileMeta object of the new file
     * @param _content Content of the file
     * @return Response from the DirectoryServer
     */
    public Message newFileToDirectory(FileMeta _fm, byte [] _content) {
        try {
            // Open socket and streams
            Socket sock = new Socket(dirServers[dirServerIndex.get()].getIP(), dirServers[dirServerIndex.get()].getPort());
            final ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
            final ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());

            // Create request, add FileMeta and content then send
            Message req = new Message("NEWFILE");
            req.addContent(String.format("%s:%d", Utils.getIP(), port));
            req.addContent(_fm);
            req.addContent(_content);
            oos.writeObject(req);

            // Get response
            Message resp = (Message) ois.readObject();

            // Close streams and socket
            oos.close();
            ois.close();
            sock.close();
            // Return response
            return resp;
        }
        // DirectoryServer is down. Try switching.
        catch (Exception e) {
            switchServer();
            return newFileToDirectory(_fm, _content);
        }
    }
}

