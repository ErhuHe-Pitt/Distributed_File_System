import java.io.*;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class TestClient extends Thread {
    private Test t;
    private String job;
    private Address [] dirServers;
    private String storageNodeIP;
    private int storageNodePort;
    private AtomicInteger dirServerIndex;
    private int nodeTried;
    private Random rand;

    public TestClient(Test _t, String _job) {
        t = _t;
        job = _job;
        dirServers = new Address [] {new Address(Utils.getIP(), 13000), new Address(Utils.getIP(), 13001)};
        rand = new Random();
        storageNodeIP = Utils.getIP();
        storageNodePort = 14000 + rand.nextInt(t.NUMBER_OF_STORAGE_NODES);
        dirServerIndex = new AtomicInteger(0);
        nodeTried = 0;
    }

    @Override
    public void run() {
        try {
            if (job.equals("UPLOAD")) {
                uploadFile();
            } else if (job.equals("DOWNLOAD")) {
                downloadFile();
            } else if (job.equals("FILELIST:SERVER")) {
                getFileListFromServer();
            } else {
                getFileListFromNode();
            }
        } catch (Exception e) {
            // Job failed.
        } finally {
            t.increaseCompleted();
        }
    }

    /**
     * Uploads a fake file to a StorageNode
     */
    public void uploadFile() {
        try {
            // Open socket and streams to send the file to the StorageNode
            Socket sock = new Socket(storageNodeIP, storageNodePort);
            final ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
            final ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());

            // Create a request
            Message req = new Message("NEWFILE_FAKE");
            int size = rand.nextInt(100000) + 100000;
            FileMeta fm = new FileMeta(String.format("fake_file_%d.txt", rand.nextInt(100000)), size);
            byte [] buff = new byte[size];
            rand.nextBytes(buff);
            req.addContent(fm);
            req.addContent(buff);

            long start = System.currentTimeMillis();
            oos.writeObject(req);

            // Get response
            Message resp = (Message) ois.readObject();
            long end = System.currentTimeMillis();

            // Close streams and sockets
            oos.close();
            ois.close();
            sock.close();

            t.addUploadTime(end - start);
            t.addTotalBytes(Utils.calculateBytes(req) + Utils.calculateBytes(resp));
            t.increaseMessages(2);
        }
        // Probably StorageNode is down, connect another one
        catch (Exception e) {
            if (nodeTried < 2) {
                nodeTried++;
                connect();
                uploadFile();
            }
        }
    }

    /**
     * Downloads a fake file from storage node
     */
    public void downloadFile() {
        try {
            // Open socket and streams to download the file from the StorageNode
            Socket sock = new Socket(storageNodeIP, storageNodePort);
            final ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
            final ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());

            // Create and send the request
            Message req = new Message("DOWNLOAD_FAKE");
            long start = System.currentTimeMillis();
            oos.writeObject(req);

            // Get response
            Message resp = (Message) ois.readObject();
            long end = System.currentTimeMillis();

            // Close streams and socket
            oos.close();
            ois.close();
            sock.close();

            t.addDowloadTimes(end - start);
            t.addTotalBytes(Utils.calculateBytes(req) + Utils.calculateBytes(resp));
            t.increaseMessages(2);
        }
        // DirectoryServer is probably down. Connect to another one.
        catch (Exception e) {
            if (nodeTried < 2) {
                nodeTried++;
                connect();
                downloadFile();
            }
        }
    }

    /**
     * Gets file list from the DirectoryServer
     */
    public void getFileListFromServer() {
        try {
            // Open socket and streams
            Socket sock = new Socket(dirServers[dirServerIndex.get()].getIP(), dirServers[dirServerIndex.get()].getPort());
            final ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
            final ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());

            // Create request and send
            Message req = new Message("FILELIST");
            long start = System.currentTimeMillis();
            oos.writeObject(req);

            // Get response
            Message resp = (Message) ois.readObject();
            long end = System.currentTimeMillis();

            // Close streams and socket
            oos.close();
            ois.close();
            sock.close();

            t.addListDir(end - start);
            t.addTotalBytes(Utils.calculateBytes(req) + Utils.calculateBytes(resp));
            t.increaseMessages(2);
        }
        // DirectoryServer is down, try to switch to backup
        catch (Exception e) {
            if (switchServer()) getFileListFromServer();
            else {
                System.out.println("Both DirectoryServers are down 1.");
                return;
            }
        }
    }

    /**
     * Gets file list from a StorageNode
     */
    public void getFileListFromNode() {
        try {
            // Open socket and streams
            Socket sock = new Socket(storageNodeIP, storageNodePort);
            final ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
            final ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());

            // Create and send the request
            Message req = new Message("FILELIST");
            long start = System.currentTimeMillis();
            oos.writeObject(req);
            // Get response
            Message resp = (Message) ois.readObject();
            long end = System.currentTimeMillis();

            // Close streams and socket
            oos.close();
            ois.close();
            sock.close();

            t.addListNode(end - start);
            t.addTotalBytes(Utils.calculateBytes(req) + Utils.calculateBytes(resp));
            t.increaseMessages(2);
        }
        // StorageNode is probably down. Connect to another.
        catch (Exception e) {
            if (nodeTried < 2) {
                nodeTried++;
                connect();
                getFileListFromNode();
            }
        }
    }

    /**
     * Makes a connect request to the DirectoryServer
     */
    public void connect() {
        try {
            // Open socket and streams
            Socket sock = new Socket(dirServers[dirServerIndex.get()].getIP(), dirServers[dirServerIndex.get()].getPort());
            final ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
            final ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());

            // Create request and send
            Message req = new Message("CONNECT");
            oos.writeObject(req);

            // Get response
            Message resp = (Message) ois.readObject();
            // If successful, get StorageNode info
            if (resp.getMessage().equals("SUCCESS")) {
                String [] addr = ((String) resp.getContent().get(0)).split(":");
                storageNodeIP = addr[0];
                storageNodePort = Integer.parseInt(addr[1]);
            }
            // Print what went wrong
            else {
                System.out.println(resp.getContent().get(0));
            }

            // Close streams and socket
            oos.close();
            ois.close();
            sock.close();

            t.addTotalBytes(Utils.calculateBytes(req) + Utils.calculateBytes(resp));
            t.increaseMessages(2);
        }
        // DirectoryServer is down. Try to switch to backup.
        catch (Exception e) {
            if (switchServer()) connect();
            else {
                System.out.println("Both DirectoryServers are down 2.");
                return;
            }
        }
    }

    /**
     * Checks if there is any backup server to switch
     * @return true if there is a backup server left, false otherwise
     */
    public boolean switchServer() {
        if (!dirServerIndex.compareAndSet(0, 1)) return false;
        return true;
    }
}
