import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract server class
 * Holds port, ID and type information
 * @author Mesut Erhan Unal and Erhu He
 */
public abstract class Server {
    protected int port;
    protected String serverID;
    protected List<Long> responseTimes;
    protected ConcurrentHashMap<String, FileMeta> fileMap;

    /**
     * Default constructor
     * @param _port Port number
     * @param _serverID ID of the server that distinguish this process from the others
     */
    public Server(int _port, String _serverID) {
        port = _port;
        serverID = _serverID;
        fileMap = new ConcurrentHashMap<String, FileMeta>();
        responseTimes = Collections.synchronizedList(new ArrayList<Long>());
    }

    /**
     * Abstract start method
     */
    abstract void start();

    /**
     * Response times list getter
     * @return response times
     */
    public List<Long> getResponseTimes() {
        return responseTimes;
    }

    /**
     * Adds a response time to the response times list
     * @param _rt Response time to add
     */
    public void addResponseTime(long _rt) {
        responseTimes.add(_rt);
    }

    /**
     * Adds a file to the file map
     * @param _meta FileMeta object of the file
     */
    public void addFile(FileMeta _meta) {
        fileMap.putIfAbsent(_meta.getName(), _meta);
    }

    /**
     * Checks if a file presents in the file map
     * @param _file File name to check
     * @return true if file name in the map, false otherwise
     */
    public boolean fileExists(String _file) {
        return fileMap.containsKey(_file);
    }

    /**
     * Returns the list of FileMeta objects which uploaded to the system
     * @return List of FileMeta objects
     */
    public ArrayList<FileMeta> getFileList() {
        return new ArrayList<FileMeta>(fileMap.values());
    }

    /**
     * Returns the ConcurrentHashMap that holds <K: filename, V: meta> entries
     * @return file map
     */
    public ConcurrentHashMap<String, FileMeta> getFileMap() {
        return fileMap;
    }

    /**
     * Returns the FileMeta object of a file
     * @param _file File name whose FileMeta object to be returned
     * @return FileMeta object
     */
    public FileMeta getFileMeta(String _file) {
        return fileMap.get(_file);
    }

    /**
     * Port number getter
     * @return Port number
     */
    public int getPort() {
        return port;
    }

    /**
     * ServerID getter
     * @return ServerID
     */
    public String getServerID() {
        return serverID;
    }

    /**
     * Fills file map with virtual files in test
     * @throws Exception if cannot read files.txt
     */
    public void fillMap() throws Exception {
        BufferedReader bf = new BufferedReader(new FileReader("./files.txt"));
        String line;
        String [] lineArr;

        while ((line = bf.readLine()) != null) {
            lineArr = line.split(",");
            addFile(new FileMeta(lineArr[0], Long.parseLong(lineArr[1])));
        }

        bf.close();
    }
}
