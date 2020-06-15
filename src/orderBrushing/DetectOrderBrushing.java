package orderBrushing;

import java.util.Date;
import java.util.TreeSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

/**
 * OrderBrushOrder is a lazy execution package for detecting order brushing.
 * When a new transaction occurs, the package reviews recent transaction
 * records, and calculate recent transaction concentration (orders / users). if
 * this number is greater or equal to 3, related transactions are deemed as
 * suspicious order brushing transactions.
 * <p>
 * The class has two main APIs:
 * <ul>
 * <li>{@code void processNewRecord(String recordLine)} is invoked when a new
 * transaction occurs. Pass transaction record OrderId, ShopId, UserId, and
 * transaction time {@code (YYYY-MM-dd HH:mm:ss)} as a string separated by
 * commas to method {@code processNewRecord()} to update the system.</li>
 * <li>Use {@code HashMap<Long, Long[]> getSuspiciousShopUser()} to retrieve
 * suspicious shopId and userId. The structure of the returned hashMap is is a
 * mapping from ShopId to an array of suspicious UserId. If the shop has no
 * suspicious transactions, corresponding the array is empty.</li>
 * </ul>
 * </p>
 */
public final class DetectOrderBrushing {

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // the main data recorded
    private final ShopList shopList = new ShopList();

    /**
     * Assume that new records come in time order, update orderBrushInformation
     *
     * @param line a line of string in the format of:
     *             orderId,shopId,userId,yyyy-MM-dd HH:mm:ss
     * @throws IllegalArgumentException if the number of elements in the line != 4
     * @throws ParseException           if orderId, shopId, or userId cannot be
     *                                  parsed to long
     * @throws NumberFormatException    if the date format is invalid
     */
    public final void processNewRecord(String recordLine) throws ParseException {
        shopList.update(parseLine(recordLine));
    }

    /**
     * retrieve suspicious shopId and top suspicious userId related to order
     * brushing in a {@code HashMap<Long, Long[]>}.
     *
     * @return a {@code Hashmap} from shopId to an array of suspicious userId
     */
    public final HashMap<Long, Long[]> getSuspiciousShopUser() {

        final HashMap<Long, Long[]> suspiciousShopUser = new HashMap<>();

        // a temporary container used repeatedly.
        final TreeSet<Long> tempSet = new TreeSet<>();

        // Must use a deep copy of shopList because this flushes the recentRecords
        // earlier than possible should be.
        for (Shop shop : shopList.clone().getShopInfo()) {
            // finish the remaining
            if (shop.isPreviousBrushOrder) {
                shop.isPreviousBrushOrder = false;
                for (Record r : shop.recentRecords) {
                    Integer count = shop.suspiciousTransactionCount.get(r.userId);
                    shop.suspiciousTransactionCount.put(r.userId, count + 1);
                }
                shop.recentRecords.clear();
            }

            // find the maximum order brushing number among users
            int max = 0;
            for (Integer count : shop.suspiciousTransactionCount.values()) {
                if (count > max) {
                    max = count;
                }
            }

            // get usersId and put in ascending order
            tempSet.clear();
            if (max > 0) {
                for (Long userId : shop.suspiciousTransactionCount.keySet()) {
                    if (shop.suspiciousTransactionCount.get(userId) == max)
                        tempSet.add(userId);
                }
            }
            suspiciousShopUser.put(shop.shopId, tempSet.toArray(new Long[tempSet.size()]));
        }
        return suspiciousShopUser;
    }

    /**
     * parse a CSV string line into a transaction record .
     *
     * @param line a line of string in the format of:
     *             orderId,shopId,userId,yyyy-MM-dd HH:mm:ss
     * @return Record converted from this line of string.
     * @throws IllegalArgumentException if the number of elements in the line != 4
     * @throws ParseException           if orderId, shopId, or userId cannot be
     *                                  parsed to long
     * @throws NumberFormatException    if the date format is invalid
     */
    public static Record parseLine(String line) throws ParseException {
        String[] temp = line.split(",");
        if (temp.length != 4) {
            throw new IllegalArgumentException("Wrong number of elements in line");
        }
        long orderId = Long.parseLong(temp[0].strip());
        long shopId = Long.parseLong(temp[1].strip());
        long userId = Long.parseLong(temp[2].strip());
        Date eventTime = DATE_FORMAT.parse(temp[3].strip());
        return new Record(orderId, shopId, userId, eventTime);
    }
}
