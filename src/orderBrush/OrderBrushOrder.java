package orderBrush;

import java.util.Date;
import java.util.TreeSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayDeque;
import java.util.Queue;

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
public final class OrderBrushOrder {

    public static final long ONE_HOUR = 1000 * 60 * 60;
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final ShopList shopList = new ShopList();

    /**
     * Information of shops including shop id, recent transactions, and number of
     * suspicious Transactions related to each user.
     */
    private final class Shop {

        long shopId;
        private ArrayDeque<Record> internalRecords = new ArrayDeque<>();

        // recent transaction records
        Queue<Record> recentRecords = internalRecords;
        // clock records the scan position (always one hour before latest transaction time)
        Date clock = null;
        // isPreviousBrushOrder determines whether order-brushing is on-going
        boolean isPreviousBrushOrder = false;
        // map from userId to number of suspicious transactions
        HashMap<Long, Integer> suspiciousTransactionCount = new HashMap<>();
        // number of orders last hour, calculate concentration only when this number changes
        int numberOfOrdersLastHour = 0;

        Shop(long id) {
            shopId = id;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Shop clone() {
            Shop shopCopy = new Shop(shopId);
            shopCopy.recentRecords = internalRecords.clone();
            shopCopy.clock = clock;
            shopCopy.isPreviousBrushOrder = isPreviousBrushOrder;
            shopCopy.suspiciousTransactionCount = (HashMap<Long, Integer>) suspiciousTransactionCount.clone();
            shopCopy.numberOfOrdersLastHour = numberOfOrdersLastHour;
            return shopCopy;
        }
    }

    /**
     * The list of shops and the information related to recent transactions
     */
    private final class ShopList {

        // from shopId to shopInfo
        HashMap<Long, Shop> shopList = new HashMap<>();

        /**
         * the main method to update the suspicious list
         *
         * @param record put new transaction here record
         */
        public final void update(Record record) {

            // calculate one hour before the transaction time
            final Date oneHourBefore = new Date(record.eventTime.getTime() - ONE_HOUR);

            // initialize if this is a new shop to the list
            if (!shopList.containsKey(record.shopId)) {
                shopList.put(record.shopId, new Shop(record.shopId));
            }

            // initialize if this is a new user to the shop
            Shop shop = shopList.get(record.shopId);
            if (!shop.suspiciousTransactionCount.containsKey(record.userId)) {
                shop.suspiciousTransactionCount.put(record.userId, 0);
            }
            if (shop.clock == null) {
                shop.recentRecords.add(record);
                shop.clock = oneHourBefore;
                return;
            }

            // scan forward to detect high concentration till latest time
            while (shop.clock.compareTo(oneHourBefore) < 0) {
                shop.clock = new Date(shop.clock.getTime() + 1000);
                detect(shop, record, false);
            }

            // add new record and detect again
            shop.recentRecords.add(record);
            shop.numberOfOrdersLastHour++;
            detect(shop, record, true);
        }

        /**
         * called when one of the two things happens: either a new record is added into
         * recentRecords, or clock advance by one second.
         */
        private void detect(Shop shop, Record record, boolean newRecordAdded) {

            // if this is a clock advancement event (no new record added):
            // 1. remove unnecessary records from recentRecords.
            // 2. if the orders in the last hour does not change, return.
            if (!newRecordAdded) {

                // Remove records that are older than one hour if order-brushing is not going
                // on, but keep all records when order-brushing is going on.
                if (!shop.isPreviousBrushOrder) {
                    while (!shop.recentRecords.isEmpty()
                            && shop.recentRecords.peek().eventTime.compareTo(shop.clock) < 0) {
                        shop.recentRecords.remove();
                    }
                }

                // calculate numberOfOrdersLastHour
                int numberOfOrdersLastHour = 0;
                if (shop.recentRecords.isEmpty() || shop.recentRecords.peek().eventTime.compareTo(shop.clock) >= 0) {
                    numberOfOrdersLastHour = shop.recentRecords.size();
                } else {
                    int numberOfOutdatedOrders = 0;
                    for (Record r : shop.recentRecords) {
                        if (r.eventTime.compareTo(shop.clock) >= 0) {
                            break;
                        }
                        numberOfOutdatedOrders++;
                    }
                    numberOfOrdersLastHour = shop.recentRecords.size() - numberOfOutdatedOrders;
                }

                // If number of orders last hour does not change, there is no need for
                // recalculation since the concentration does not change, as clock-advance
                // does not insert new records into recentRecords.
                if (numberOfOrdersLastHour == shop.numberOfOrdersLastHour) {
                    return;
                } else {
                    // update shop.numberOfOrdersLastHour
                    shop.numberOfOrdersLastHour = numberOfOrdersLastHour;
                }
            }

            // calculate concentration of the shop.
            // if the concentration >= 3, enter isPreviousBrushOrder = true period
            if (concentration(shop) >= 3) {
                shop.isPreviousBrushOrder = true;
                return;
            }

            // Else if concentration < 3, but previous concentration > 3, an order-brushing
            // period has just ended, record all suspicious activities.
            if (shop.isPreviousBrushOrder) {
                for (Record r : shop.recentRecords) {
                    // skip new record since it is not order-brushing
                    if (newRecordAdded && r.equals(record)) {
                        continue;
                    }
                    // increment suspicious action for corresponding users
                    Integer count = shop.suspiciousTransactionCount.get(r.userId);
                    shop.suspiciousTransactionCount.put(r.userId, count + 1);
                }
                shop.recentRecords.clear();
                // add new record back
                if (newRecordAdded) shop.recentRecords.add(record);
                // reset isPreviousBrushOrder
                shop.isPreviousBrushOrder = false;
            }
        }

        public final Collection<Shop> getShopInfo() {
            return shopList.values();
        }

        /**
         * make a deep copy of ShopList, used for query
         */
        @Override
        public ShopList clone() {
            ShopList shopListCopy = new ShopList();
            for (Shop shop : shopList.values()) {
                shopListCopy.shopList.put(shop.shopId, shop.clone());
            }
            return shopListCopy;
        }
    }

    /**
     * Calculate the concentration of last hour (time indicated by shop.clock).
     */
    private static final int concentration(Shop shop) {
        HashSet<Long> users = new HashSet<>();
        final Queue<Record> recentRecords = shop.recentRecords;
        final Date clock = shop.clock;
        assert clock != null;
        int transactionThisHour = 0;
        for (Record r : recentRecords) {
            if (clock.compareTo(r.eventTime) <= 0) {
                users.add(r.userId);
                transactionThisHour++;
            }
        }
        if (users.size() == 0) {
            return 0;
        }
        return transactionThisHour / users.size();
    }

    /**
     * assume that new records come in time order, update orderBrushInformation
     *
     * @param the input line, fields separated by comma
     */
    public final void processNewRecord(String recordLine) throws ParseException {
        shopList.update(parseLine(recordLine));
    }

    /**
     * pull the suspicious list
     *
     * @return a {@code Hashmap} from shopId to an array containing suspicious users
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
                    if (shop.suspiciousTransactionCount.get(userId) == max) tempSet.add(userId);
                }
            }
            suspiciousShopUser.put(shop.shopId, tempSet.toArray(new Long[tempSet.size()]));
        }
        return suspiciousShopUser;
    }

    /**
     * parse a CSV string line into a transaction record
     */
    public static Record parseLine(String line) throws ParseException {
        String[] temp = line.split(",");
        long orderId = Long.parseLong(temp[0].strip());
        long shopId = Long.parseLong(temp[1].strip());
        long userId = Long.parseLong(temp[2].strip());
        Date eventTime = DATE_FORMAT.parse(temp[3].strip());
        return new Record(orderId, shopId, userId, eventTime);
    }
}
