package orderBrushing;

import java.util.Date;
import java.util.ArrayDeque;
import java.util.HashMap;

/**
 * Information of shops including shopId, recentOrders, and number of suspicious
 * Transactions related to each user.
 */
final class Shop {

    /*** fields of shop information ***/

    final long shopId;
    final ArrayDeque<Order> recentOrders;

    // storing suspiciousUsers of the shop and the number of suspicious transactions
    // associated to each user.
    HashMap<Long, Integer> suspiciousUsers = new HashMap<>();


    /*** fields to aid calculation ***/

    // clock memorize the scan position (one hour before latest transaction time)
    Date clock = null;
    // isPreviousBrushOrder determines whether order-brushing is on-going
    boolean isPreviousBrushOrder = false;
    // number of orders last hour
    int numberOfOrdersLastHour = 0;

    Shop(long id) {
        shopId = id;
        recentOrders = new ArrayDeque<>();
    }

    /**
     * The {@code clone()} method of Shop is not a simple shallow copy, but also
     * invokes clone() of one critical field:
     * {@code HashMap<Long, Integer> suspiciousUsers}. Since suspiciousUsers is a
     * mapping with immutable keys and values, clone() gives a deep copy of
     * suspiciousUsers. There is no need to clone recentOrders for our purpose, as
     * it is read only in {@code ShopList.update()}.
     */
    @Override
    @SuppressWarnings("unchecked")
    public final Shop clone() {
        try {
            Shop shopCopy = (Shop) super.clone();
            shopCopy.suspiciousUsers = (HashMap<Long, Integer>) suspiciousUsers.clone();
            return shopCopy;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError("Shop clone error");
        }
    }
}
