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
 * <li>{@code void processNewOrder(String orderLine)} is invoked when a new
 * transaction occurs. Pass transaction record OrderId, ShopId, UserId, and
 * transaction time {@code (YYYY-MM-dd HH:mm:ss)} as a string separated by
 * commas to method {@code processNewOrder()} to update the system.</li>
 * <li>Use {@code HashMap<Long, Long[]> getSuspiciousShopUser()} to retrieve
 * suspicious shopId and userId. The structure of the returned hashMap is a
 * mapping from ShopId to an array of suspicious UserId. If the shop has no
 * suspicious transactions, the corresponding array is empty.</li>
 * </ul>
 * </p>
 */
public final class DetectOrderBrushing {

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // the main data of the class
    private final ShopList shopList = new ShopList();

    /**
     * Update the system. Require new orders to come <em>in time order</em>. Should
     * be invoked each time a new order occurs.
     *
     * @param orderLine a line of string in the format of:
     *                  {@code orderId,shopId,userId,yyyy-MM-dd HH:mm:ss}
     * @throws IllegalArgumentException if the number of elements in the line != 4
     * @throws ParseException           if the date format is invalid
     * @throws NumberFormatException    if orderId, shopId, or userId are illegal
     */
    public final void processNewOrder(String orderLine) throws ParseException {
        shopList.update(parseLine(orderLine));
    }

    /**
     * Retrieve suspicious shopId and userId in a {@code HashMap<Long, Long[]>}. The
     * Keys are shopId, and the Values are arrays of userId who are suspicious of
     * conducting the highest number of order brushing. If the shop is not deemed as
     * suspicious, the array of suspicious users has length 0.
     *
     * @return a {@code Hashmap} from shopId to an array of suspicious userId
     */
    public final HashMap<Long, Long[]> getSuspiciousShopUser() {

        final HashMap<Long, Long[]> suspiciousShopUser = new HashMap<>();

        // a temporary container used repeatedly.
        final TreeSet<Long> tempSet = new TreeSet<>();

        // Must use a deep copy of shopList because this flushes the recentOrders
        // earlier than possible should be.
        for (Shop shop : shopList.getShopInfo()) {

            // find the maximum order brushing number among users
            int max = 0;
            for (Integer count : shop.suspiciousUsers.values()) {
                if (count > max) {
                    max = count;
                }
            }

            // get usersId and put in ascending order
            tempSet.clear();
            for (Long userId : shop.suspiciousUsers.keySet()) {
                if (shop.suspiciousUsers.get(userId) == max)
                    tempSet.add(userId);
            }
            suspiciousShopUser.put(shop.shopId, tempSet.toArray(new Long[0]));
        }
        return suspiciousShopUser;
    }

    /**
     * This is a utility method to parse a string into an Order object.
     *
     * @param line a line of string in the format of:
     *             {@code orderId,shopId,userId,yyyy-MM-dd HH:mm:ss}
     * @return Order converted from this line of string.
     * @throws IllegalArgumentException if the number of elements in the line != 4
     * @throws ParseException           if the date format is invalid
     * @throws NumberFormatException    if orderId, shopId, or userId are illegal
     */
    public static Order parseLine(String line) throws ParseException {
        String[] temp = line.split(",");
        if (temp.length != 4) {
            throw new IllegalArgumentException("Wrong number of elements in line");
        }
        long orderId = Long.parseLong(temp[0].strip());
        long shopId = Long.parseLong(temp[1].strip());
        long userId = Long.parseLong(temp[2].strip());
        Date eventTime = DATE_FORMAT.parse(temp[3].strip());
        return new Order(orderId, shopId, userId, eventTime);
    }
}
