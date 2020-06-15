package orderBrush;

import java.util.Date;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Queue;

/**
 * Information of shops including shop id, recent transactions, and number of
 * suspicious Transactions related to each user.
 */
final class Shop {

    long shopId;
    private ArrayDeque<Record> internalRecords = new ArrayDeque<>();

    // recent transaction records
    Queue<Record> recentRecords = internalRecords;

    // clock records the scan position (one hour before latest transaction time)
    Date clock = null;

    // isPreviousBrushOrder determines whether order-brushing is on-going
    boolean isPreviousBrushOrder = false;

    // map from userId to number of suspicious transactions
    HashMap<Long, Integer> suspiciousTransactionCount = new HashMap<>();

    // number of orders last hour
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
