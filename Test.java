import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test class is to test systems with varying parameters
 */
public class Test {
    private int M;
    private int N;
    private int F;
    private Random rand;
    private AtomicInteger completed;
    private List<String> jobQueue;
    private List<Long> totalBytes;

    private List<Long> listFileTimesDir;
    private List<Long> listFileTimesNode;
    private List<Long> uploadTimes;
    private List<Long> downloadTimes;
    private AtomicInteger messages;

    final int NUMBER_OF_STORAGE_NODES = 20;
    private final int FAILED_NODE = 0;
    private final boolean FAIL_DIRECTORY = true;
    private final int NUMBER_OF_CONCURRENT_THREADS = 50;

    /**
     * Entry point for the Test
     * @param args: M, N and F(in this order)
     */
    public static void main(String [] args) {
        if (args.length != 3) {
            System.out.println("Test expects 3 inputs");
            System.out.println("1) Number of files in the system");
            System.out.println("2) Number of requests");
            System.out.println("3) Frequency of requests (in a second). 0 for concurrent requests");
            System.exit(0);
        }

        // Instantiate a new Test object and start the test
        Test t = new Test(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        t.start();
    }

    /**
     * Default constructor
     * @param _M: Number of files
     * @param _N: Number of requests
     * @param _F: Frequency of requests
     */
    public Test(int _M, int _N, int _F) {
        M = _M;
        N = _N;
        F = _F;
        completed = new AtomicInteger(0);
        rand = new Random();
        jobQueue = Collections.synchronizedList(new ArrayList<String>());
        listFileTimesDir = Collections.synchronizedList(new ArrayList<Long>());
        listFileTimesNode = Collections.synchronizedList(new ArrayList<Long>());
        totalBytes = Collections.synchronizedList(new ArrayList<Long>());
        uploadTimes = Collections.synchronizedList(new ArrayList<Long>());
        downloadTimes = Collections.synchronizedList(new ArrayList<Long>());
        messages = new AtomicInteger(0);
    }

    public void start() {
        try {
            writeTestFile();
            fillQueue();

            // Start a DirectoryServer and backup on the port 13000 and 13001
            Process primary = Runtime.getRuntime().exec(String.format("java RunDirectoryServer 13000 testserver %s:13001 true true", Utils.getIP()));
            Process backup = Runtime.getRuntime().exec("java RunDirectoryServer 13001 backup false true");

            // Wait 3 seconds for servers to start
            Thread.sleep(3000);

            // Start StorageNodes
            ArrayList<Process> nodes = new ArrayList<Process>();
            for (int i = 0; i < NUMBER_OF_STORAGE_NODES; i++)
                nodes.add(Runtime.getRuntime().exec(String.format("java RunStorageNode %d storage%d %s:13000 %s:13001 true", 14000 + i, i, Utils.getIP(), Utils.getIP())));

            System.out.println("Test is starting....");

            // Wait another 3 seconds for primary server sends its state to backup
            Thread.sleep(3000);

            // Are we testing with failed directory server? If so, kill directory server process
            if (FAIL_DIRECTORY)
                primary.destroy();

            // Are we testing with failed nodes? If so, kill first N StorageNodes
            if (FAILED_NODE > 0)
                for (int i = 0; i < FAILED_NODE; i++)
                    nodes.get(i).destroy();

            // Create a thread pool to execute jobs
            final ExecutorService pool = Executors.newFixedThreadPool(NUMBER_OF_CONCURRENT_THREADS);
            String job;
            TestClient tc = null;

            // Assign each job in the queue to a TestClient thread
            for (int i = 0; i < jobQueue.size(); i++) {
                if (F > 0) Thread.sleep(1000 / F); // Sleep 1000/F ms
                tc = new TestClient(this, jobQueue.get(i));
                pool.execute(tc);
            }

            // Wait until all threads are done
            while(completed.get() != N) Thread.sleep(10);

            System.out.println("Test finished...\n\n");

            long totalFileListDir = 0;
            long totalFileListNode = 0;
            long totalSize = 0;
            long totalUploadTimes = 0;
            long totalDownloadTimes = 0;

            for (int i = 0; i < listFileTimesDir.size(); i++)
                totalFileListDir += listFileTimesDir.get(i);
            for (int i = 0; i < listFileTimesNode.size(); i++)
                totalFileListNode += listFileTimesNode.get(i);
            for (int i = 0; i < totalBytes.size(); i++)
                totalSize += totalBytes.get(i);
            for (int i = 0; i < uploadTimes.size(); i++)
                totalUploadTimes += uploadTimes.get(i);
            for (int i = 0; i < downloadTimes.size(); i++)
                totalDownloadTimes += downloadTimes.get(i);

            // Print out the statistics
            System.out.println("STATISTICS\n-------------------\n");
            System.out.printf("LIST requests to DirectoryServer: %d, Avg response: %.4f\n", listFileTimesDir.size(), (totalFileListDir * 1.0) / listFileTimesDir.size());
            System.out.printf("LIST requests to StorageNodes: %d, Avg response: %.4f\n", listFileTimesNode.size(), (totalFileListNode * 1.0) / listFileTimesNode.size());
            System.out.printf("UPLOAD requests: %d, Avg response: %.4f\n", uploadTimes.size(), (totalUploadTimes * 1.0) / uploadTimes.size());
            System.out.printf("DOWNLOAD requests: %d, Avg response: %.4f\n", downloadTimes.size(), (totalDownloadTimes * 1.0) / downloadTimes.size());
            System.out.printf("Total bytes transferred: %d\n", totalSize);
            System.out.printf("Messages exchanged: %d\n", messages.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Increase completed job count
     */
    public void increaseCompleted() {
        completed.incrementAndGet();
    }

    /**
     * Increases exchanged message counter by M
     * @param _m Increment amount
     */
    public void increaseMessages(int _m) {
        messages.addAndGet(_m);
    }

    /**
     * Adds into lookupTimes list
     * @param _t lookup time to be added
     */
    public void addListDir(long _t) {
        listFileTimesDir.add(_t);
    }

    /**
     * Adds into lookupTimes list
     * @param _t lookup time to be added
     */
    public void addListNode(long _t) {
        listFileTimesNode.add(_t);
    }

    /**
     * Adds into totalBytes list
     * @param _b byte count to be added
     */
    public void addTotalBytes(long _b) {
        totalBytes.add(_b);
    }

    /**
     * Adds into uploadTimes list
     * @param _ut upload time to add
     */
    public void addUploadTime(long _ut) {
        uploadTimes.add(_ut);
    }

    /**
     * Adds into downloadTimes list
     * @param _dt download time to add
     */
    public void addDowloadTimes(long _dt) {
        downloadTimes.add(_dt);
    }

    /**
     * Create a virtual job queue with list file calls
     * 80% list files (~20% of them to DirectoryServer), 10% upload and 10% download
     */
    private void fillQueue() {
        int random;
        for (int i = 0; i < (int) (N * 0.8); i++) {
            random = rand.nextInt(5);
            jobQueue.add(random == 0 ? "FILELIST:SERVER" : "FILELIST:NODE");
        }

        for (int i = 0; i < (int) (N * 0.1); i++)
            jobQueue.add("DOWNLOAD");
        for (int i = 0; i < (int) (N * 0.1); i++)
            jobQueue.add("UPLOAD");
    }

    /**
     * Write a virtual test file
     * This test file will be read by DirectoryServer and
     * StorageNodes and fill their file maps
     */
    private void writeTestFile() {
        try {
            BufferedWriter buf = new BufferedWriter(new FileWriter("files.txt"));
            int filesize = 0;

            for (int i = 0; i < M; i++) {
                filesize = rand.nextInt(100000);
                buf.write(String.format("file%d.txt,%d\n", i, filesize));
            }

            buf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
