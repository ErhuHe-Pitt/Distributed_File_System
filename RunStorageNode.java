/**
 * Entry point for StorageNode
 * @author Erhu He
 */
public class RunStorageNode {
    public static void main(String [] args) {
        if (args.length != 5) {
            System.out.println("RunStorageNode expects 5 arguments");
            System.out.println("1) Port Number");
            System.out.println("2) Server ID (any string that can distinguish this server)");
            System.out.println("3) Primary DirectoryServer address (e.g. 127.12.122.12:1234)");
            System.out.println("4) Backup DirectoryServer address (e.g. 127.12.122.12:1234)");
            System.out.println("5) Test flag");
            System.exit(0);
        }

        String [] pAddr = args[2].split(":");
        String [] bAddr = args[3].split(":");
        StorageNode sn = new StorageNode(Integer.parseInt(args[0]), args[1], pAddr[0], Integer.parseInt(pAddr[1]), bAddr[0], Integer.parseInt(bAddr[1]), Boolean.parseBoolean(args[4]));
        sn.start();
    }
}
