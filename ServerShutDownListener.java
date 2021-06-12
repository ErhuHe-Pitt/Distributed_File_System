import java.io.BufferedWriter;
import java.io.FileWriter;

/**
 * Shutdown listener class
 */
class ServerShutDownListener extends Thread {
    public Server server;

    public ServerShutDownListener (Server _server) {
        server = _server;
    }

    /**
     * Overridden run function which will be invoked when we shut down DirectoryServer or StorageNode
     * Calculates statistics and prints them out. Also writes them into Logs/{serverID}.log.
     */
    @Override
    public void run() {
        System.out.println("Shutting down the DirectoryServer\n");
        int totalRequests = server.getResponseTimes().size();
        long totalTime = 0;
        for (long t : server.getResponseTimes())
            totalTime += t;
        System.out.println("Statistics:");
        System.out.println(String.format("Total File List Requests: %d", totalRequests));
        System.out.println(String.format("Average Response Time: %.4f", (totalTime * 1.0) / totalRequests));

        try {
            BufferedWriter bf = new BufferedWriter(new FileWriter(String.format("./Logs/%s.log", server.getServerID())));
            bf.write(String.format("Total File List Requests: %d\n", totalRequests));
            bf.write(String.format("Average Response Time: %.4f\n", (totalTime * 1.0) / totalRequests));
            bf.close();
        } catch (Exception e) {
            System.out.println(String.format("Could not write in ./Logs/%s.log", server.getServerID()));
            e.printStackTrace();
        }
    }
}
