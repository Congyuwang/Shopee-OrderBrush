package orderBrushing;

import java.util.Date;
import java.util.ArrayDeque;
import java.util.HashMap;

/**
 * Information of shops including shop id, recent transactions, and number of
 * suspicious Transactions related to each user.
 */
final class Shop {

    final long shopId;
    final ArrayDeque<Order> recentOrders;

    // clock memorize the scan position (one hour before latest transaction time)
    Date clock = null;

    // isPreviousBrushOrder determines whether order-brushing is on-going
    boolean isPreviousBrushOrder = false;

    // map from userId to number of suspicious transactions involved
    HashMap<Long, Integer> suspiciousUsers = new HashMap<>();

    // number of orders last hour
    int numberOfOrdersLastHour = 0;

    Shop(long id) {
        shopId = id;
        recentOrders = new ArrayDeque<>();
    }

    private Shop(long id, ArrayDeque<Order> orders) {
        shopId = id;
        recentOrders = orders;
    }

    @Override
    @SuppressWarnings("unchecked")
    public final Shop clone() {
        Shop shopCopy = new Shop(shopId, recentOrders.clone());
        shopCopy.clock = clock;
        shopCopy.isPreviousBrushOrder = isPreviousBrushOrder;
        shopCopy.suspiciousUsers = (HashMap<Long, Integer>) suspiciousUsers.clone();
        shopCopy.numberOfOrdersLastHour = numberOfOrdersLastHour;
        return shopCopy;
    }
}
