package orderBrush;

import java.util.Date;
import java.util.TreeSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;

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
            info.recentRecords.add(record);
            info.lastOneHour.add(record);
            if (!info.suspiciousTransactionCount.containsKey(record.userId)) {
                info.suspiciousTransactionCount.put(record.userId, 0);
            }

            // keep records if brush detected
            if (!info.isPreviousBrushOrder) {
                while (!info.recentRecords.isEmpty()
                        && info.recentRecords.peek().eventTime.compareTo(oneHourBefore) < 0) {
                    info.recentRecords.remove();
                }
            }

            // remove old transactions from lastOneHour
            while (!info.lastOneHour.isEmpty() && info.lastOneHour.peek().eventTime.compareTo(oneHourBefore) < 0) {
                info.lastOneHour.remove();
            }

            // record suspicious activity
            if (concentration(info.lastOneHour) >= 3) {
                info.isPreviousBrushOrder = true;
            } else if (info.isPreviousBrushOrder) {
                info.isPreviousBrushOrder = false;
                for (Record r : info.recentRecords) {
                    if (!r.equals(record)) {
                        Integer count = info.suspiciousTransactionCount.get(r.userId);
                        info.suspiciousTransactionCount.put(r.userId, count + 1);
                    }
                }
                info.recentRecords.clear();
                info.recentRecords.add(record);
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
        long orderId = Long.parseLong(temp[0]);
        long shopId = Long.parseLong(temp[1]);
        long userId = Long.parseLong(temp[2]);
        Date eventTime = DATE_FORMAT.parse(temp[3]);
        return new Record(orderId, shopId, userId, eventTime);
    }
}
