import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * DirectoryServer is the server which maintain file consistency
 * among all storage nodes. It listens a specific port for upcoming connections
 * and pass them a DirectoryServerThread
 * @author Mesut Erhan Unal and Erhu He
 */
public class DirectoryServer extends Server {
    private String backupIP;
    private int backupPort;
    private ConcurrentHashMap<String, Boolean> nodeMap;
    private ConcurrentLinkedQueue<Job> jobs;
    private boolean primary;
    private boolean test;
    private Random rand;
    private AtomicBoolean backupJobThreadStarted;

    /**
     * Backup constructor
     * @param _port Port number to listen
     * @param _serverID Unique server ID
     * @param _primary Primary server flag
     * @param _test Test mode flag
     */
    public DirectoryServer(int _port, String _serverID, boolean _primary, boolean _test) {
        super(_port, _serverID);
        backupIP = null;
        backupPort = 0;
        nodeMap = new ConcurrentHashMap<String, Boolean>();
        jobs = new ConcurrentLinkedQueue<Job>();
        primary = _primary;
        test = _test;
        rand = new Random();
        backupJobThreadStarted = new AtomicBoolean(false);
    }

    /**
     * Primary server constructor
     * @param _port Port to listen
     * @param _serverID Unique server ID
     * @param _backupIP Backup server's IP
     * @param _backupPort Backup server's port
     * @param _primary Primary server flag
     * @param _test Test mode flag
     */
    public DirectoryServer(int _port, String _serverID, String _backupIP, int _backupPort, boolean _primary, boolean _test) {
        super(_port, _serverID);
        backupIP = _backupIP;
        backupPort = _backupPort;
        nodeMap = new ConcurrentHashMap<String, Boolean>();
        jobs = new ConcurrentLinkedQueue<Job>();
        primary = _primary;
        test = _test;
        rand = new Random();
    }

    @Override
    public void start() {
        try {
            if (test) fillMap();

            // Add a shutdown hook
            Runtime runtime = Runtime.getRuntime();
            runtime.addShutdownHook(new ServerShutDownListener(this));

            // Set a backup daemon if this is primary
            if (primary) {
                Backup backupThread = new Backup(this);
                backupThread.setDaemon(true);
                backupThread.start();
            }

            // If primary server, set job reducer daemon
            if (primary) {
                JobThread jobReducer = new JobThread(this);
                jobReducer.setDaemon(true);
                jobReducer.start();
            }

            // Open a ServerSocket and start listening
            final ServerSocket serverSock = new ServerSocket(port);
            System.out.println(String.format("DirectoryServer is running on %s:%d", Utils.getIP(), port));

            Socket sock = null;
            DirectoryServerThread t = null;

            while (true) {
                sock = serverSock.accept();
                t = new DirectoryServerThread(sock, this);
                t.start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Add a StorageNode to node map
     * @param _node StorageNode to add
     */
    public void addNode(String _node) {
        nodeMap.put(_node, true);
    }

    /**
     * Make a StorageNode unavailable
     * @param _node StorageNode to make unavailable
     */
    public void setNodeUnavailable(String _node) {
        nodeMap.put(_node, false);
    }

    /**
     * Check if a StorageNode exists in the system
     * @param _node StorageNode to be checked
     * @return true if it exists, false otherwise
     */
    public boolean nodeExists(String _node) {
        return nodeMap.containsKey(_node);
    }

    /**
     * Choose a random, alive StorageNode to reply Connect request from the client
     * @return an alive StorageNode
     */
    public String getRandomNode() {
        ArrayList<String> nodes = new ArrayList<String>(nodeMap.keySet());
        int size = nodes.size(), randIndex;

        while (true) {
            randIndex = rand.nextInt(size);
            if (nodeMap.get(nodes.get(randIndex))) {
                try {
                    String [] addr = nodes.get(randIndex).split(":");
                    Socket sock = new Socket(addr[0], Integer.parseInt(addr[1]));
                    final ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
                    final ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());

                    Message req = new Message("HEARTBEAT");
                    oos.writeObject(req);
                    Message resp = (Message) ois.readObject();

                    oos.close();
                    ois.close();
                    sock.close();

                    return nodes.get(randIndex);
                } catch (Exception e) {
                    setNodeUnavailable(nodes.get(randIndex));
                }
            }
        }
    }

    /**
     * Add a Job into the queue
     * @param _j Job to add
     */
    public void addJob(Job _j) {
        jobs.add(_j);
    }

    /**
     * Backup server's IP getter
     * @return Backup DirectoryServer's IP
     */
    public String getBackupIP() {
        return backupIP;
    }

    /**
     * Backup server's port getter
     * @return Backup DirectoryServer's port
     */
    public int getBackupPort() {
        return backupPort;
    }

    /**
     * Poll a job from the head of the queue
     * @return First Job from the queue
     */
    public Job getJob() {
        return jobs.poll();
    }

    /**
     * nodeMap getter
     * @return nodeMap that holds <K: address, V: alive> entries
     */
    public ConcurrentHashMap<String, Boolean> getNodeMap() {
        return nodeMap;
    }

    /**
     * Checks if this DirectoryServer is primary
     * @return true if this primary, false otherwise
     */
    public boolean isPrimary() {
        return primary;
    }

    /**
     * Flag that indicates if backup server become primary
     * @return true if backup server became the primary, false otherwise
     */
    public AtomicBoolean getBackupJobThreadStarted() {
        return backupJobThreadStarted;
    }

    /**
     * Runs the Job reducer thread
     */
    public void runJobThread() {
        JobThread jobReducer = new JobThread(this);
        jobReducer.setDaemon(true);
        jobReducer.start();
    }

    /**
     * Fills this DirectoryServer's state in a Message object
     * @return Message object that holds this object's state
     */
    public Message getState() {
        Message m = new Message("STATE");
        m.addContent(nodeMap);
        m.addContent(fileMap);
        m.addContent(jobs);
        return m;
    }

    /**
     * Sets this DirectoryServer's state from a Message object
     * @param _m Message object that holds a DirectoryServer state
     */
    public void setState(Message _m) {
        nodeMap = (ConcurrentHashMap<String, Boolean>) _m.getContent().get(0);
        fileMap = (ConcurrentHashMap<String, FileMeta>) _m.getContent().get(1);
        jobs = (ConcurrentLinkedQueue<Job>) _m.getContent().get(2);
    }

    public boolean isTest() {
        return test;
    }
}
