import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Backup thread moves primary DirectoryServer's state
 * to the backup DirectoryServer in every second.
 */
class Backup extends Thread {
    private DirectoryServer ds;

    /**
     * Default constructor
     * @param _ds Primary DirectoryServer object
     */
    public Backup(DirectoryServer _ds) {
        ds = _ds;
    }

    public void run() {
        do {
            try
            {
                // Sleep 1 sec
                Thread.sleep(1000);
                try
                {
                    // Open socket and streams to send primary's state to backup
                    Socket sock = new Socket(ds.getBackupIP(), ds.getBackupPort());
                    ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
                    ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());

                    // Write state to output stream
                    oos.writeObject(ds.getState());

                    // Get response
                    Message resp = (Message) ois.readObject();

                    if (!resp.getMessage().equals("SUCCESS")) {
                        System.err.println("Error: Backup server cannot sync the state.");
                    }

                    // Close streams and socket
                    oos.close();
                    ois.close();
                    sock.close();
                }
                // Backup server is not alive?
                catch(Exception e) {
                    System.err.println(String.format("Error: Cannot connect to backup server on %s:%d.", ds.getBackupIP(), ds.getBackupPort()));
                }
            }

            catch(Exception e) {
                System.err.println("Backup interrupted");
            }
        } while(true);
    }
}
