package orderBrushing;

import java.text.DateFormat;
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

    // the main data of the class
    private final ShopList shopList;
    private DateFormat dateFormat;

    /**
     * Construct a new order brushing detector with default parameters:
     * {@code window} = 1 hour, {@code concentrationThreshold} = 3, and
     * {@code increment} = second. The default date format is
     * {@code java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")}.
     */
    public DetectOrderBrushing() {
        this.shopList = new ShopList(60 * 60 * 1000, 3, 1000);
        this.dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    /**
     * Construct a new order brushing detector with specified parameters.
     * Transaction activities are deemed as suspicious when the transactions within
     * a given {@code window} time length (unit: milliseconds) are greater or equal
     * to the specified {@code concentrationThreshold}. The scanning algorithm
     * calculates concentration with an increment as specified by {@code increment}
     * (unit millisecond). It is recommended that increment be set to the smallest
     * time unit of a system.
     *
     * @param window                 the time length of window for calculating
     *                               concentration, in milliseconds
     * @param concentrationThreshold the minimum value of concentration for
     *                               suspicious transactions
     * @param increment              the step of scanning, the smaller the more
     *                               accurate, but more costly. Recommend to use the
     *                               smallest time unit in the system (milliseconds)
     * @param dateFormat             parse dateFormat in String (e.g. "yyyy-MM-dd
     *                               HH:mm:ss") see
     *                               {@link java.text.SimpleDateFormat} for details.
     * @throws IllegalArgumentException if window < 1, increment < 1, concentration
     *                                  < 1, or dateFormat is invalid.
     */
    public DetectOrderBrushing(long window, int concentrationThreshold, long increment, String dateFormat) {
        if (increment < 1 || window < 1 || concentrationThreshold <= 0) {
            throw new IllegalArgumentException("illegal parameter");
        }
        this.shopList = new ShopList(window, concentrationThreshold, increment);
        this.dateFormat = new SimpleDateFormat(dateFormat);
    }

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
        shopList.update(parseLine(orderLine, dateFormat));
    }

    /**
     * Retrieve suspicious shopId and userId in a {@code HashMap<Long, Long[]>}. The
     * Keys are shopId, and the Values are arrays of userId who are suspicious of
     * conducting <em>the highest number of order brushing</em>. If the shop is not
     * deemed as suspicious, the array of suspicious users has length 0.
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
     * Retrieve <b>all</b> suspicious shopId and userId in a
     * {@code HashMap<Long, Long[]>} who are deemed to have conducted number of
     * suspicious order brushing more than {@code int threshold} times. The Keys are
     * shopId, and the Values are arrays of userId who are suspicious of conducting
     * more number of order brushing than threshold. If the shop is not deemed as
     * suspicious, the array of suspicious users has length 0.
     *
     * @param threshold the smallest number of suspicious transactions for each user
     * @return a {@code Hashmap} from shopId to an array of suspicious userId
     * @throws IllegalArgumentException if threshold i < 1.
     */
    public final HashMap<Long, Long[]> getAllSuspiciousShopUser(int threshold) {

        final HashMap<Long, Long[]> suspiciousShopUser = new HashMap<>();

        // a temporary container used repeatedly.
        final TreeSet<Long> tempSet = new TreeSet<>();

        // Must use a deep copy of shopList because this flushes the recentOrders
        // earlier than possible should be.
        for (Shop shop : shopList.getShopInfo()) {

            // get usersId and put in ascending order
            tempSet.clear();
            for (Long userId : shop.suspiciousUsers.keySet()) {
                if (shop.suspiciousUsers.get(userId) >= threshold)
                    tempSet.add(userId);
            }
            suspiciousShopUser.put(shop.shopId, tempSet.toArray(new Long[0]));
        }
        return suspiciousShopUser;
    }

    /**
     * This is a utility method to parse a string into an Order object.
     *
     * @param line       a line of string in the format of:
     *                   {@code orderId,shopId,userId,yyyy-MM-dd HH:mm:ss}
     * @param dateFormat specifying the date format used for parsing date.
     * @return Order converted from this line of string.
     * @throws IllegalArgumentException if the number of elements in the line != 4
     * @throws ParseException           if the date format is invalid
     * @throws NumberFormatException    if orderId, shopId, or userId are illegal
     */
    public static Order parseLine(String line, DateFormat dateFormat) throws ParseException {
        String[] temp = line.split(",");
        if (temp.length != 4) {
            throw new IllegalArgumentException("Wrong number of elements in line");
        }
        long orderId = Long.parseLong(temp[0].strip());
        long shopId = Long.parseLong(temp[1].strip());
        long userId = Long.parseLong(temp[2].strip());
        Date eventTime = dateFormat.parse(temp[3].strip());
        return new Order(orderId, shopId, userId, eventTime);
    }

    /**
     * Change current dateFormat.
     *
     * @param dateFormat used by the system (e.g., "yyyy-MM-dd HH:mm:ss.SSS").
     */
    public void setDateFormat(String dateFormat) {
        this.dateFormat = new SimpleDateFormat(dateFormat);
    }

    /**
     * Get the current date format.
     *
     * @return dateFormat used by the system.
     */
    public DateFormat getDateFormat() {
        return dateFormat;
    }

    /**
     * Get the current window.
     *
     * @return window used by the system.
     */
    public long getWindow() {
        return shopList.getWindow();
    }

    /**
     * Get the current increment.
     *
     * @return increment used by the system.
     */
    public long getIncrement() {
        return shopList.getIncrement();
    }

    /**
     * Get the current concentrationThreshold.
     *
     * @return concentrationThreshold used by the system.
     */
    public int getConcentrationThreshold() {
        return shopList.getConcentrationThreshold();
    }

    /**
     * Change the window for concentration computation.
     *
     * @param window the size of window, in milliseconds.
     * @throws IllegalArgumentException if value smaller than 1.
     */
    public void setWindow(long window) {
        if (window < 1) {
            throw new IllegalArgumentException("window must be positive");
        }
        shopList.setWindow(window);
    }

    /**
     * Change the increment for concentration scanning.
     *
     * @param increment the size of increment, in milliseconds.
     * @throws IllegalArgumentException if value smaller than 1.
     */
    public void setIncrement(long increment) {
        if (increment < 1) {
            throw new IllegalArgumentException("increment must be positive");
        }
        shopList.setIncrement(increment);
    }

    /**
     * Change the size of concentrationThreshold.
     *
     * @param concentrationThreshold new concentrationThreshold.
     * @throws IllegalArgumentException if value smaller than 1.
     */
    public void setConcentrationThreshold(int concentrationThreshold) {
        if (concentrationThreshold < 1) {
            throw new IllegalArgumentException("concentrationThreshold must be positive");
        }
        shopList.setIncrement(concentrationThreshold);
    }
}
