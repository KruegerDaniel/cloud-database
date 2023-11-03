package de.tum.i13.shared;

import java.util.Objects;

public class Range {
    private String address;
    private int port;
    private String lowerBound;
    private String upperBound;

    /**
     * Object that stores important hashing information about the server with a specific IP and port
     * <p>
     * The hash-bounds of objects are stored in byte Arrays of size 32 and the server with a given IP is responsible
     * for the keys that are hashed between its upper and lower bound
     */
    public Range(String address, int port, String lowerBound, String upperBound) {
        this.address = address;
        this.port = port;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public static Range parseRange(String toParse) {
        String[] fields = toParse.replace(";", "").split(",");
        String[] ipNPort = fields[2].split(":");
        return new Range(ipNPort[0], Integer.parseInt(ipNPort[1]), fields[0], fields[1]);
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(String lowerBound) {
        this.lowerBound = lowerBound;
    }

    public String getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(String upperBound) {
        this.upperBound = upperBound;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Range range = (Range) o;
        return port == range.port &&
                Objects.equals(address, range.address) &&
                Objects.equals(lowerBound, range.lowerBound) &&
                Objects.equals(upperBound, range.upperBound);
    }

    @Override
    public String toString() {
        return lowerBound + "," + upperBound + "," + address + ":" + port + ";";
    }
}
