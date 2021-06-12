import java.util.ArrayList;
import java.util.Map;

/**
 * JobThread is the thread that reduces DirectoryServer's JobQueue.
 * It picks one job at a time and passes it to either Uploader or Sync
 * based on the job type.
 * @author Mesut Erhan Unal and Erhu He
 */
public class JobThread extends Thread {
    private DirectoryServer ds;

    /**
     * Default constructor
     * @param _ds DirectoryServer object whose job picked
     */
    public JobThread(DirectoryServer _ds) {
        ds = _ds;
    }

    @Override
    public void run() {
        try{
            do {
                // Get a job from DirectoryServer's job queue
                Job j = ds.getJob();

                // If queue is empty, sleep 1 sec.
                if (j == null) {
                    Thread.sleep(1000);
                    continue;
                }

                // Upload new file to other nodes
                if (j.getType().equals("SYNCFILE")) {
                    ArrayList<Uploader> uploaders = new ArrayList<Uploader>();

                    for (Map.Entry<String, Boolean> entry : ds.getNodeMap().entrySet())
                        if (!entry.getKey().equals(j.getNode()) && entry.getValue())
                            uploaders.add(new Uploader(this, ds, entry.getKey(), (FileMeta) j.getContent().get(0), (byte[]) j.getContent().get(1)));

                    for (Uploader u : uploaders)
                        u.start();
                    for (Uploader u : uploaders)
                        u.join();

                    // file synced. add to file list.
                    ds.addFile((FileMeta) j.getContent().get(0));
                }

                // Sync a new node with other nodes.
                else if (j.getType().equals("SYNCEVERYTHING")) {
                    ArrayList<Sync> syncers = new ArrayList<Sync>();

                    for (Map.Entry<String, FileMeta> entry: ds.getFileMap().entrySet())
                        syncers.add(new Sync(this, ds, j.getNode(), entry.getValue()));
                    for (Sync s : syncers)
                        s.start();
                    for (Sync s : syncers)
                        s.join();
                }
            } while (true);
        } catch (Exception e) {
            System.err.println("Job thread interrupted.");
        }
    }
}
