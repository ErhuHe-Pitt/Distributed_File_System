import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DFS Client to perform basic operations such as getting file list,
 * uploading and/or downloading a file.
 * @author Mesut Erhan Unal and Erhu He
 */
public class Client {
    private String clientID;
    private Address [] dirServers;
    private String storageNodeIP;
    private int storageNodePort;
    private AtomicInteger dirServerIndex;

    /**
     * Entry point for the Client
     * @param args Arguments passed from the console
     */
    public static void main (String [] args) {
        if (args.length != 3) {
            System.out.println("Client expects 3 arguments");
            System.out.println("1) Client ID (any string that can distinguish this client)");
            System.out.println("2) Primary DirectoryServer address (e.g. 127.12.122.12:1234)");
            System.out.println("3) Backup DirectoryServer address (e.g. 127.12.122.12:1234)");
            System.exit(0);
        }
        String [] pAddr = args[1].split(":");
        String [] bAddr = args[2].split(":");
        Client c = new Client(args[0], pAddr[0], Integer.parseInt(pAddr[1]), bAddr[0], Integer.parseInt(bAddr[1]));
        c.run();
    }

    /**
     * Default constructor
     * @param _clientID Unique client ID
     * @param _dirIP DirectoryServer's IP
     * @param _dirPort DirectoryServer's port
     * @param _backupIP Backup server's IP
     * @param _backupPort Backup server's port
     */
    public Client(String _clientID, String _dirIP, int _dirPort, String _backupIP, int _backupPort) {
        clientID = _clientID;
        dirServers = new Address [] {new Address(_dirIP, _dirPort), new Address(_backupIP, _backupPort)};
        storageNodeIP = null;
        storageNodePort = -1;
        dirServerIndex = new AtomicInteger(0);
    }

    public void run() {
        // Create a directory for the client if not already exists
        File dir = new File("./" + clientID);
        if (!dir.isDirectory()) dir.mkdir();

        // Read user input and start operating
        Scanner sc = new Scanner(System.in);
        System.out.println("Welcome to the DFS client.");
        int selection = 1;

        while (selection != 0) {
            System.out.println("1) Get file list from DirectoryServer");
            if (storageNodeIP == null) {
                System.out.println("2) Connect to a StorageNode");
            } else {
                System.out.println("2) Get file list from StorageNode");
                System.out.println("3) Upload a file");
                System.out.println("4) Read a file");
            }
            System.out.println("0) Exit");

            String file = null;

            switch (selection = Integer.parseInt(sc.nextLine())) {
                case 0:
                    System.out.println("Bye...");
                    break;
                case 1:
                    getFileListFromServer();
                    break;
                case 2:
                    if (storageNodeIP == null) connect();
                    else getFileListFromNode();
                    break;
                case 3:
                    System.out.print("Enter filename to upload: ");
                    file = sc.nextLine();
                    uploadFile(file);
                    break;
                case 4:
                    System.out.print("Enter filename to download: ");
                    file = sc.nextLine();
                    downloadFile(file);
                    break;
                default:
                    System.out.println("Wrong selection.");
                    break;
            }
        }
    }

    /**
     * Uploads a file to a StorageNode
     * @param _file Name of the file to upload
     */
    public void uploadFile(String _file) {
        if (storageNodeIP == null) {
            System.out.println("Connect to a StorageNode first.");
            return;
        }

        try {
            // Open socket and streams to send the file to the StorageNode
            Socket sock = new Socket(storageNodeIP, storageNodePort);
            final ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
            final ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());

            // Create a request
            Message req = new Message("NEWFILE");

            // Check if file exists and can be read
            File f = new File(String.format("./%s/%s", clientID, _file));
            if (!f.isFile() || !f.canRead()) {
                oos.close();
                ois.close();
                sock.close();
                System.out.println("File not found or cannot read.");
                return;
            }

            // Read file, add it to the request and send
            FileMeta fm = new FileMeta(_file, f.length());
            byte [] buff = new byte[(int) f.length()];
            FileInputStream fis = new FileInputStream(f);
            fis.read(buff);
            fis.close();
            req.addContent(fm);
            req.addContent(buff);
            oos.writeObject(req);

            // Get response
            Message resp = (Message) ois.readObject();

            if (resp.getMessage().equals("SUCCESS")) {
                System.out.println("Uploaded successfully.");
            } else {
                System.out.println(resp.getContent().get(0));
            }

            // Close streams and sockets
            oos.close();
            ois.close();
            sock.close();
        }
        // Probably StorageNode is down, connect another one
        catch (Exception e) {
            connect();
            uploadFile(_file);
        }
    }

    /**
     * Downloads a file from a StorageNode
     * @param _file Name of the file to download
     */
    public void downloadFile(String _file) {
        if (storageNodeIP == null) {
            System.out.println("Connect to a StorageNode first.");
            return;
        }

        try {
            // Open socket and streams to download the file from the StorageNode
            Socket sock = new Socket(storageNodeIP, storageNodePort);
            final ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
            final ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());

            // Create and send the request
            Message req = new Message("DOWNLOAD");
            req.addContent(_file);
            oos.writeObject(req);

            // Get response
            Message resp = (Message) ois.readObject();
            // If successful, write file in this client's directory
            if (resp.getMessage().equals("SUCCESS")) {
                FileMeta fm = (FileMeta) resp.getContent().get(0);
                byte [] content = (byte []) resp.getContent().get(1);
                FileOutputStream fos = new FileOutputStream(String.format("./%s/%s", clientID, fm.getName()));
                fos.write(content);
                fos.close();
                System.out.println("File downloaded.");
            }
            // Print what went wrong
            else {
                System.out.println(resp.getContent().get(0));
            }

            // Close streams and socket
            oos.close();
            ois.close();
            sock.close();
        }
        // DirectoryServer is probably down. Connect to another one.
        catch (Exception e) {
            connect();
            downloadFile(_file);
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
            oos.writeObject(req);

            // Get response
            Message resp = (Message) ois.readObject();
            // If successful print the list of files
            if (resp.getMessage().equals("SUCCESS")) {
                printFiles((ArrayList<FileMeta>) resp.getContent().get(0));
            }
            // Print what went wrong
            else {
                System.out.println(resp.getContent().get(0));
            }

            // Close streams and socket
            oos.close();
            ois.close();
            sock.close();
        }
        // DirectoryServer is down, try to switch to backup
        catch (Exception e) {
            if (switchServer()) getFileListFromServer();
            else System.out.println("Both DirectoryServers are down.");
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
            oos.writeObject(req);
            // Get response
            Message resp = (Message) ois.readObject();
            // If successful, print the list of files
            if (resp.getMessage().equals("SUCCESS")) {
                printFiles((ArrayList<FileMeta>) resp.getContent().get(0));
            }
            // Print what went wrong
            else {
                System.out.println(resp.getContent().get(0));
            }

            // Close streams and socket
            oos.close();
            ois.close();
            sock.close();
        }
        // StorageNode is probably down. Connect to another.
        catch (Exception e) {
            connect();
            getFileListFromNode();
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
                System.out.println("Connected to " + resp.getContent().get(0));
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
        }
        // DirectoryServer is down. Try to switch to backup.
        catch (Exception e) {
            if (switchServer()) getFileListFromServer();
            else System.out.println("Both DirectoryServers are down.");
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

    /**
     * Prints file list in a pretty format
     * @param _files File list to be printed
     */
    public void printFiles(ArrayList<FileMeta> _files) {
        if (_files.size() == 0) {
            System.out.println("There is no file to show.");
        } else {
            System.out.printf("%-40s%30s\n", "File Name", "File Size");
            System.out.printf("%-40s%30s\n", "--------------------", "--------------------");

            for (int i = 0; i < _files.size(); i++)
                System.out.printf("%-40s%30s\n", _files.get(i).getName(), _files.get(i).getSize() + " bytes");
        }
    }
}
