/**
 * Entry point for DirectoryServer
 * @author Erhu He
 */
public class RunDirectoryServer {
    public static void main(String [] args) {
        if (args.length != 4 && args.length != 5) {
            System.out.println("RunDirectoryServer expects 4 or 5 arguments:");
            System.out.println("1) Port Number");
            System.out.println("2) Server ID (any string that can distinguish this server)");
            System.out.println("3) Backup server's address, if this is primary server (e.g. 192.157.1.12:2323)");
            System.out.println("4) Primary server flag");
            System.out.println("5) Test flag");
            System.exit(0);
        }

        DirectoryServer ds;

        if (args.length == 4) {
            ds = new DirectoryServer(Integer.parseInt(args[0]), args[1], Boolean.parseBoolean(args[2]), Boolean.parseBoolean(args[3]));
        } else {
            String [] addr = args[2].split(":");
            ds = new DirectoryServer(Integer.parseInt(args[0]), args[1], addr[0], Integer.parseInt(addr[1]), Boolean.parseBoolean(args[3]), Boolean.parseBoolean(args[4]));
        }

        ds.start();
    }
}
