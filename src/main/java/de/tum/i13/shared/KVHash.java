package de.tum.i13.shared;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class KVHash {
    private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);

    /**
     * This is a very evil algorithm that assigns hash ranges to every address and port provided
     * <p>
     * The ranges will be circular, in order to cover all the key hashes
     *
     * @param addresses The List of addresses and ports that determine the keyrange the server is responsible for
     * @return A List of {@link Range} objects that contain the server's ip and port,
     * and the values its responsible for
     */
    public static List<Range> hashMetaData(List<Pair<String, Integer>> addresses) {
        List<Range> ranges = new ArrayList<>();

        TreeMap<String, Pair<Pair<String, Integer>, byte[]>> treeMap = new TreeMap<>();
        for (Pair<String, Integer> address : addresses) {
            byte[] byteHash = hashKey(address.getLeft() + address.getRight());
            String stringHash = bytesToHex(byteHash);

            treeMap.put(stringHash, new Pair<>(address, byteHash));
        }
        for (Map.Entry<String, Pair<Pair<String, Integer>, byte[]>> stringPairEntry : treeMap.entrySet()) {
            ranges.add(
                    new Range(
                            stringPairEntry.getValue().getLeft().getLeft(),
                            stringPairEntry.getValue().getLeft().getRight(),
                            "", // updating this afterwards
                            stringPairEntry.getKey()
                    )
            );
        }

        for (int i = 0; i < ranges.size(); i++) {
            ranges.get(i).setLowerBound(bytesToHex(incrementByteArr(
                    treeMap.get(ranges.get(Math.floorMod(i - 1, ranges.size())).getUpperBound()).getRight())));
        }
        return ranges;
    }

    /**
     * Increments a byte array that represents a numbered value (which can not be stored in a java number)
     * and does it in a way that rolls over bits if necessary (e.g. inc({0x0, 0xF}) will be {0x1,0x0})
     * <p>
     * In case the byte array has the highest possible value, the result will be 0
     */
    public static byte[] incrementByteArr(byte[] arr) {
        for (int i = arr.length - 1; i >= 0; --i) {
            if (arr[i] != -1) {
                arr[i]++;
                break;
            } else {
                arr[i] = 0;
            }
        }
        return arr;
    }

    /**
     * Uses the MD5 Algorithm of the {@link MessageDigest} class to hash a
     * specific String to a byte array with length 32
     */
    public static byte[] hashKey(String key) {
        MessageDigest messageDigest = null;
        try {
            messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ignored) {
        }
        messageDigest.update(key.getBytes());
        return messageDigest.digest();
    }

    /**
     * Converts a byte-Array to a hexString with uppercase letters
     *
     * @author https://stackoverflow.com/a/9855338/10269919
     */
    public static String bytesToHex(byte[] bytes) {
        byte[] hexChars = new byte[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars, StandardCharsets.UTF_8);
    }

    public static boolean isHash(String key) {
        if (key.length() != 32) {
            return false;
        }
        for (byte ch : key.getBytes()) {
            if ((ch < '0' || ch > '9')
                    && (ch < 'A' || ch > 'F')) {
                return false;
            }
        }
        return true;
    }

    public static List<Range> calculateReplicatorRange(List<Range> metadata) {
        List<Range> replicatorRange = new ArrayList<>();
        if (metadata.size() < 3) {
            return metadata;
        }
        for (int i = 0; i < metadata.size(); i++) {
            replicatorRange.add(new Range(metadata.get(i).getAddress(),
                    metadata.get(i).getPort(),
                    metadata.get(Math.floorMod(i - 2, metadata.size())).getLowerBound(),
                    metadata.get(i).getUpperBound()));
        }
        return replicatorRange;
    }

    /**
     * Determines whether the provided (hashed) key is in the bounds between upper (inc) and lower (exc)
     *
     * @param key   The String value of a key after its been hashed to MD5
     * @param lower the lower bound to compare against
     * @param upper the upper boud to compare agains
     * @return true if key is in the circular range of lower and upper
     */
    public static boolean inRange(String key, String lower, String upper) {
        if (lower == null && upper == null) {
            return false;
        }
        if (lower.compareTo(upper) < 0) { // lower is smaller than upper (no wraparound)
            return key.compareTo(lower) > 0 && key.compareTo(upper) <= 0;
        } else {
            return (key.compareTo(lower) > 0 && key.compareTo(upper) >= 0)
                    || (key.compareTo(lower) < 0 && key.compareTo(upper) <= 0);
        }
    }
}
