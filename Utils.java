import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 * Utils class for common methods
 * @author Erhu He
 */
public class Utils {
    public static void main(String [] args) {
        System.out.println("Suktur");
    }
    /**
     * Calculate byte size of an object
     * @param _o: Object whose size to be calculated
     * @return Size
     */
    public static long calculateBytes(Object _o) {
        long total = 0;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(_o);
            oos.flush();
            oos.close();
            total = baos.toByteArray().length;
            baos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return total;
    }

    /**
     * Calculates IP address of the machine. If InetAddress gives the loopback (127.0.0.1)
     * it opens a UDP socket to 8.8.8.8 and captures machine's global IP
     * @return IP address of the machine
     */
    public static String getIP() {
        try {
            String ip = InetAddress.getLocalHost().getHostAddress().trim();
            if (!ip.equals("127.0.0.1")) return ip;
            final DatagramSocket socket = new DatagramSocket();
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            return socket.getLocalAddress().getHostAddress();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}