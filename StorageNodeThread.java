import java.io.*;
import java.net.Socket;
import java.util.Random;

/**
 * StorageNodeThread class to respond each incoming connection
 * to the StorageNode.
 * @author Mesut Erhan Unal and Erhu He
 */
public class StorageNodeThread extends Thread {
    private Socket sock;
    private StorageNode server;
    private Random rand;

    /**
     * Default constructor
     * @param _sock Socket object
     * @param _server StorageNode object that spawned this thread
     */
    public StorageNodeThread(Socket _sock, StorageNode _server) {
        sock = _sock;
        server = _server;
        rand = new Random();
    }

    @Override
    public void start() {
        try {
            // Open streams
            final ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());
            final ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());

            // Read request
            Message request = (Message) ois.readObject();
            Message response = null;

            // DirectoryServer wants to give a file
            if (request.getMessage().equals("UPLOAD")) {
                FileMeta fm = (FileMeta) request.getContent().get(0);
                byte [] content = (byte []) request.getContent().get(1);
                FileOutputStream fos = new FileOutputStream(String.format("./%s/%s", server.getServerID(), fm.getName()));
                fos.write(content);
                fos.close();
                server.addFile(fm);
                response = new Message("SUCCESS");
            }

            // Client uploads a new file
            else if (request.getMessage().equals("NEWFILE")) {
                FileMeta fm = (FileMeta) request.getContent().get(0);
                byte [] content = (byte []) request.getContent().get(1);
                Message resp = server.newFileToDirectory(fm, content);

                if (resp.getMessage().equals("SUCCESS")) {
                    response = new Message("SUCCESS");
                    FileOutputStream fos = new FileOutputStream(String.format("./%s/%s", server.getServerID(), fm.getName()));
                    fos.write(content);
                    fos.close();
                    server.addFile(fm);
                } else {
                    response = new Message("FAIL");
                    response.addContent(resp.getContent().get(0));
                }
            }

            // Client wants to download a file
            else if (request.getMessage().equals("DOWNLOAD")) {
                String file = (String) request.getContent().get(0);
                if (server.fileExists(file)) {
                    FileMeta fm = server.getFileMeta(file);
                    byte [] content = new byte [(int) fm.getSize()];

                    FileInputStream fis = new FileInputStream(String.format("./%s/%s", server.getServerID(), fm.getName()));
                    fis.read(content);
                    fis.close();

                    response = new Message("SUCCESS");
                    response.addContent(fm);
                    response.addContent(content);
                } else {
                    response = new Message("FAIL");
                    response.addContent("File not found in the storage node.");
                }
            }

            // Client wants to get file list
            else if (request.getMessage().equals("FILELIST")) {
                long start = System.currentTimeMillis();
                response = new Message("SUCCESS");
                response.addContent(server.getFileList());
                long end = System.currentTimeMillis();
                server.addResponseTime(end - start);
            }

            // Fake file upload for testing
            else if (request.getMessage().equals("NEWFILE_FAKE")) {
                // do nothing.
                response = new Message("SUCCESS");
            }

            // Fake file download for testing
            else if (request.getMessage().equals("DOWNLOAD_FAKE")) {
                int size = rand.nextInt(100000) + 100000;
                byte [] content = new byte[size];
                rand.nextBytes(content);
                response = new Message("SUCCESS");
                response.addContent(new FileMeta("fake_file.txt", size));
                response.addContent(content);
            }

            // Bad request
            else {
                response = new Message("FAIL");
                response.addContent("Bad request");
            }

            // Write response to output stream
            oos.writeObject(response);

            // Close streams and sockets
            ois.close();
            oos.close();
            sock.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
