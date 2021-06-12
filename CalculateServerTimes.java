import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Helper class to calculate response times measured by
 * DirectoryServers and StorageNodes after a test completed.
 */
public class CalculateServerTimes {
    private double directoryServers;
    private double storageNodes;

    public static void main(String [] args) {
        CalculateServerTimes c = new CalculateServerTimes();
        try {
            c.calculateDirectory();
            c.calculateNodes();
            c.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public CalculateServerTimes() {
        directoryServers = .0;
        storageNodes = .0;
    }

    public void calculateDirectory() throws Exception {
        double temp = .0;
        String line;
        String [] s;
        int reqP = 0;
        int reqB = 0;
        BufferedReader br = new BufferedReader(new FileReader("./Logs/testserver.log"));
        for (int i = 0; i < 2; i++) {
            s = br.readLine().split(":");
            if (i == 0) reqP = Integer.parseInt(s[1].trim());
            else
                if (reqP > 0) temp += reqP * Double.parseDouble(s[1].trim());
        }

        br.close();

        br = new BufferedReader(new FileReader("./Logs/backup.log"));
        for (int i = 0; i < 2; i++) {
            s = br.readLine().split(":");
            if (i == 0) reqB = Integer.parseInt(s[1].trim());
            else
            if (reqB > 0) temp += reqB * Double.parseDouble(s[1].trim());
        }
        br.close();

        directoryServers = temp / (reqB + reqP);
    }

    public void calculateNodes() throws Exception {
        double temp = .0;
        int totalReq = 0;

        File logDir = new File("./Logs");
        for (final File f : logDir.listFiles()) {
            if (f.isFile() && f.getName().matches("storage\\d+\\.log")) {
                int req = 0;
                String [] s;
                BufferedReader br = new BufferedReader(new FileReader("./Logs/" + f.getName()));
                for (int i = 0; i < 2; i++) {
                    s = br.readLine().split(":");
                    if (i == 0) {
                        req = Integer.parseInt(s[1].trim());
                        totalReq += req;
                    } else {
                        if (req > 0)
                            temp += req * Double.parseDouble(s[1].trim());
                    }
                }

                br.close();
            }
        }
        System.out.println("total storage req " + totalReq);
        storageNodes = temp / totalReq;
    }

    public void print() {
        System.out.printf("DirectoryServer avg: %.4f\n", directoryServers);
        System.out.printf("StorageNode avg: %.4f\n", storageNodes);
    }
}
