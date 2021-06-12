/**
 * A basic class to hold IP, port pairs
 * @author Mesut Erhan Unal and Erhu He
 */
public class Address {
    private String IP;
    private int port;

    /**
     * Default constuctor
     * @param _IP IP address
     * @param _port Port number
     */
    public Address(String _IP, int _port) {
        IP = _IP;
        port = _port;
    }

    /**
     * IP getter
     * @return IP address
     */
    public String getIP() {
        return IP;
    }

    /**
     * Port getter
     * @return Port number
     */
    public int getPort() {
        return port;
    }
}
