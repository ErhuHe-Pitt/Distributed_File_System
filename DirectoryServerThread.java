import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * DirectoryServerThread is the class which to be instantiated
 * for each incoming connection to the DirectoryServer
 * @author Mesut Erhan Unal and Erhu He
 */
public class DirectoryServerThread extends Thread {
    private Socket sock;
    private DirectoryServer server;

    /**
     * Default constructor
     * @param _sock Socket object of the connection
     * @param _server DirectoryServer object which instantiated this object
     */
    public DirectoryServerThread(Socket _sock, DirectoryServer _server) {
        sock = _sock;
        server = _server;
    }

    @Override
    public void run() {
        try {
            // Open streams
            final ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());
            final ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());

            // Get request and create an empty response object
            Message request = (Message) ois.readObject();
            Message response = null;

            // Register request from a StorageNode
            if (request.getMessage().equals("REGISTER")) {
                String node = (String) request.getContent().get(0);
                server.addNode(node);
                response = new Message("SUCCESS");
                if (!server.isTest())
                    server.addJob(new Job("SYNCEVERYTHING", node));
                System.out.println("A storage node has been registered.");
            }
            // New file add request from a StorageNode
            else if (request.getMessage().equals("NEWFILE")) {
                String node = (String) request.getContent().get(0);
                FileMeta fm = (FileMeta) request.getContent().get(1);
                byte [] content = (byte []) request.getContent().get(2);

                if (server.fileExists(fm.getName())) {
                    response = new Message("FAIL");
                    response.addContent("This file already exists in the system.");
                }

                else {
                    Job j = new Job("SYNCFILE", node);
                    j.addContent(fm);
                    j.addContent(content);
                    server.addJob(j);
                    response = new Message("SUCCESS");
                }
            }
            // Connect request from a Client
            else if (request.getMessage().equals("CONNECT")) {
                response = new Message("SUCCESS");
                response.addContent(server.getRandomNode());
            }
            // List files request from a Client
            else if (request.getMessage().equals("FILELIST")) {
                long start = System.currentTimeMillis();
                response = new Message("SUCCESS");
                response.addContent(server.getFileList());
                long end = System.currentTimeMillis();
                server.addResponseTime(end - start);
            }
            // State copy request from the primary DirectoryServer
            else if (request.getMessage().equals("STATE")) {
                server.setState(request);
                response = new Message("SUCCESS");
            }
            // Some other requests
            else {
                response = new Message("FAIL");
                response.addContent("Bad request");
            }
            // If DirectoryServer was backup server but primary is down
            // make it primary
            if (!request.getMessage().equals("STATE") && !server.isPrimary() && server.getBackupJobThreadStarted().compareAndSet(false, true)) {
                server.runJobThread();
            }

            // Write response object to output stream
            oos.writeObject(response);

            // Close streams and socket
            ois.close();
            oos.close();
            sock.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
