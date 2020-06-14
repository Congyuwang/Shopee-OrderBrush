package orderBrush;

import java.util.Date;
import java.util.TreeSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;

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
    private final class ShopInfo {
        long shopId;

        // recent transactions
        PriorityQueue<Record> recentRecords = new PriorityQueue<>(Record.TIME_COMPARATOR);
        PriorityQueue<Record> lastOneHour = new PriorityQueue<>(Record.TIME_COMPARATOR);

        // fields to aid computation:
        // clock records the latest algorithm scan position for each shop
        Date clock = null;
        // isPreviousBrushOrder determines whether order-brushing is on-going
        boolean isPreviousBrushOrder = false;

        // map from userId to number of suspicious transactions
        HashMap<Long, Integer> suspiciousTransactionCount = new HashMap<>();

        ShopInfo(long id) {
            shopId = id;
        }
    }

    /**
     * The list of shops and the information related to recent transactions
     */
    private final class ShopList {

        // from shopId to shopInfo
        HashMap<Long, ShopInfo> shopList = new HashMap<>();

        /**
         * the main method to update the suspicious list
         *
         * @param record put new transaction here record
         */
        public final void update(Record record) {

            final Date oneHourBefore = new Date(record.eventTime.getTime() - ONE_HOUR);

            // get shopInfo and add new transaction
            if (!shopList.containsKey(record.shopId)) {
                shopList.put(record.shopId, new ShopInfo(record.shopId));
            }
            ShopInfo info = shopList.get(record.shopId);
            if (!info.suspiciousTransactionCount.containsKey(record.userId)) {
                info.suspiciousTransactionCount.put(record.userId, 0);
            }

            if (info.clock == null) {
                info.recentRecords.add(record);
                info.lastOneHour.add(record);
                info.clock = oneHourBefore;
                return;
            }

            // scan and advance time to latest event
            boolean needsRecalculate[] = new boolean[] {false};
            while (info.clock.compareTo(oneHourBefore) < 0) {
                info.clock = new Date(info.clock.getTime() + 1000);
                core(info, record, needsRecalculate, false);
            }

            // add new record and recalculate
            info.recentRecords.add(record);
            info.lastOneHour.add(record);
            needsRecalculate[0] = true;
            core(info, record, needsRecalculate, true);
        }

        private void core(ShopInfo info, Record record, boolean[] needsRecalculate, boolean newRecordAdded) {
            // keep records if brush detected
            if (!info.isPreviousBrushOrder) {
                while (!info.recentRecords.isEmpty() && info.recentRecords.peek().eventTime.compareTo(info.clock) < 0) {
                    info.recentRecords.remove();
                }
            }

            // remove old transactions from lastOneHour
            while (!info.lastOneHour.isEmpty() && info.lastOneHour.peek().eventTime.compareTo(info.clock) < 0) {
                info.lastOneHour.remove();
                needsRecalculate[0] = true;
            }

            // record suspicious activity
            if (needsRecalculate[0]) {
                if (concentration(info.lastOneHour) >= 3) {
                    info.isPreviousBrushOrder = true;
                } else if (info.isPreviousBrushOrder) {
                    info.isPreviousBrushOrder = false;
                    for (Record r : info.recentRecords) {
                        if (!newRecordAdded || !r.equals(record)) {
                            Integer count = info.suspiciousTransactionCount.get(r.userId);
                            info.suspiciousTransactionCount.put(r.userId, count + 1);
                        }
                    }
                    info.recentRecords.clear();
                    if (newRecordAdded) {
                        info.recentRecords.add(record);
                    }
                }
                needsRecalculate[0] = false;
            }
        }

        public final Collection<ShopInfo> getShopInfo() {
            return shopList.values();
        }
    }

    /**
     * Calculate the concentration within the recentTransactions
     */
    private static final int concentration(PriorityQueue<Record> recentRecords) {
        HashSet<Long> users = new HashSet<>();
        for (Record r : recentRecords) {
            users.add(r.userId);
        }
        if (users.size() == 0) {
            return 0;
        }
        return recentRecords.size() / users.size();
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
        HashMap<Long, Long[]> suspiciousShopUser = new HashMap<>();
        TreeSet<Long> tempSet = new TreeSet<>();

        // find suspicious user for each shop
        for (ShopInfo info : shopList.getShopInfo()) {
            // finish the remaining
            if (info.isPreviousBrushOrder) {
                info.isPreviousBrushOrder = false;
                for (Record r : info.recentRecords) {
                    Integer count = info.suspiciousTransactionCount.get(r.userId);
                    info.suspiciousTransactionCount.put(r.userId, count + 1);
                }
                info.recentRecords.clear();
            }

            int max = 0;
            tempSet.clear();
            for (Integer count : info.suspiciousTransactionCount.values()) {
                if (count > max) {
                    max = count;
                };
            }
            if (max > 0) {
                for (Long userId : info.suspiciousTransactionCount.keySet()) {
                    if (info.suspiciousTransactionCount.get(userId) == max) tempSet.add(userId);
                }
            }
            suspiciousShopUser.put(info.shopId, tempSet.toArray(new Long[tempSet.size()]));
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
