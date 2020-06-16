package orderBrushing;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Queue;

/**
 * The ShopList class consists of a list of shops and is the main processor for
 * detecting orderBrushing activities.
 * <p>
 * The core algorithm is written in the following methods:
 * <ul>
 * <li>{@code void update(Order order)} receives a new order, scans
 * concentration rates, and update suspicious user list for the corresponding
 * shop.</li>
 * <li>{@code int detect(Shop shop, Order order, boolean newOrderAdded)} is a
 * worker method used in update() that does a lazy evaluation of concentration
 * rate, and updates suspicious user list.</li>
 * <li>{@code Collection<Shop> getShopInfo()} makes a deep copy of shopList, and
 * pour the remaining suspicious users into the suspicious user list. It returns
 * a collection of cloned shops with latest information.</li>
 * </ul>
 * </p>
 */
final class ShopList {

    // model parameters
    private long window;
    private long increment;
    private int concentrationThreshold;

    // map from shopId to Shop
    private final HashMap<Long, Shop> shopList;

    /**
     * Construct a ShopList with default parameters: deem concentration greater than
     * {@code concentrationThreshold} in {@code window} milliseconds as suspicious
     * order brushing.
     *
     * @param window                 the length of window for calculating concentration.
     * @param concentrationThreshold the smallest concentration to be deemed as
     *                               suspicious transactions.
     * @param increment              the increment of scan. The smaller then more
     *                               accurate. recommended to be set as the smallest
     *                               unit time in the system.
     * @throws IllegalArgumentException if increment <= 0 or if window <= 0 or
     *                                  concentrationThreshold <= 0
     */
    ShopList(long window, int concentrationThreshold, long increment) {
        this.window = 1000 * 60 * 60;
        this.concentrationThreshold = 3;
        this.increment = increment;
        this.shopList = new HashMap<>();
    }

    /**
     * the main method to update the suspicious list, the orders must be input
     * <em>according to time order</em>.
     *
     * @param order put a new order here
     */
    final void update(Order order) {

        // calculate one hour before the latest transaction time
        final Date windowLowerBound = new Date(order.eventTime.getTime() - window);

        // initialize if this is a new shop to the list
        if (!shopList.containsKey(order.shopId)) {
            shopList.put(order.shopId, new Shop(order.shopId));
        }
        Shop shop = shopList.get(order.shopId);
        if (shop.clock == null) {
            shop.recentOrders.add(order);
            shop.clock = windowLowerBound;
            return;
        }

        // scan forward by one second until latest time
        while (shop.clock.compareTo(windowLowerBound) < 0) {

            // fast forward if numberOfOrdersLastHour is smaller than concentrationThreshold
            if (detect(shop, order, false) < concentrationThreshold) {
                shop.clock = windowLowerBound;
                break;
            }

            // increment time
            shop.clock = new Date(shop.clock.getTime() + increment);
        }

        // add new order and detect again
        shop.recentOrders.add(order);
        shop.numberOfOrdersLastHour++;
        detect(shop, order, true);
    }

    /**
     * called when one of the two things happens: either a new order is added into
     * recentOrders, or clock advance by one second.
     *
     * @return the number of orders in the last hour
     */
    private int detect(Shop shop, Order order, boolean newOrderAdded) {

        // if this is a clock advancement event (no new order added):
        // 1. remove unnecessary orders from recentOrders.
        // 2. if the orders in the last hour does not change, return.
        int numberOfOrdersLastHour = 0;
        if (!newOrderAdded) {

            // Remove orders that are older than one hour if order-brushing is not going
            // on, but keep all orders when order-brushing is going on.
            if (!shop.isPreviousBrushOrder) {
                while (!shop.recentOrders.isEmpty() && shop.recentOrders.peek().eventTime.compareTo(shop.clock) < 0) {
                    shop.recentOrders.remove();
                }
            }

            // calculate the number of orders last hour
            if (shop.recentOrders.isEmpty() || shop.recentOrders.peek().eventTime.compareTo(shop.clock) >= 0) {

                // in this case there is no orders older than one hour
                numberOfOrdersLastHour = shop.recentOrders.size();
            } else {

                // find out the number of orders older than one hour first
                int numberOfOutdatedOrders = 0;
                for (Order r : shop.recentOrders) {
                    if (r.eventTime.compareTo(shop.clock) >= 0) {
                        break;
                    }
                    numberOfOutdatedOrders++;
                }
                numberOfOrdersLastHour = shop.recentOrders.size() - numberOfOutdatedOrders;
            }

            // If number of orders last hour does not change, there is no need for
            // recalculation since the concentration does not change. Clock-advance
            // does not insert new orders into recentOrders.
            if (numberOfOrdersLastHour == shop.numberOfOrdersLastHour) {
                return numberOfOrdersLastHour;
            }
            shop.numberOfOrdersLastHour = numberOfOrdersLastHour;
        }

        // if the concentration >= concentrationThreshold, let isPreviousBrushOrder = true and return.
        if (concentration(shop) >= concentrationThreshold) {
            shop.isPreviousBrushOrder = true;
            return numberOfOrdersLastHour;
        }

        // if concentration < concentrationThreshold, and previous concentration < 3, return.
        if (!shop.isPreviousBrushOrder) {
            return numberOfOrdersLastHour;
        }

        // if concentration < concentrationThreshold, and previous concentration >=
        // concentrationThreshold, an order-brushing period has just ended. Pour all
        // suspicious activities into {@code suspiciousTransactionCount} and clear
        // recentOrders.
        for (Order r : shop.recentOrders) {

            // skip the new order since it is occurs when concentration < 3
            if (newOrderAdded && r.equals(order)) {
                continue;
            }

            // increment suspicious transaction count for suspicious userId
            if (!shop.suspiciousUsers.containsKey(r.userId)) {
                shop.suspiciousUsers.put(r.userId, 1);
            } else {
                Integer count = shop.suspiciousUsers.get(r.userId);
                shop.suspiciousUsers.put(r.userId, count + 1);
            }
        }
        shop.recentOrders.clear();

        // add the new order back since it is removed in clear()
        if (newOrderAdded)
            shop.recentOrders.add(order);

        // reset isPreviousBrushOrder
        shop.isPreviousBrushOrder = false;

        return numberOfOrdersLastHour;
    }

    /**
     * Make a deep copy of shopListCopy, pour all the remaining recent Orders if
     * they are deemed suspicious. Thus it does not disrupt future {@code update()}
     * processes. If the deep copy is not made, {@code getShopInfo()} would conduct
     * an early pour, which disrupts future {@code update()} process.
     *
     * @return a {@code Collection} containing all shop information.
     */
    final Collection<Shop> getShopInfo() {

        // make a deep copy
        HashMap<Long, Shop> shopListCopy = deepCopy().shopList;

        // for each shop
        for (Shop shop : shopListCopy.values()) {

            // if not order brushing, skip.
            if (!shop.isPreviousBrushOrder) {
                continue;
            }

            // if order brushing, pour the remaining suspicious transactions.
            // DO NOT clear shop.recentOrders, it is a shallow copy from shopList!
            for (Order r : shop.recentOrders) {
                if (!shop.suspiciousUsers.containsKey(r.userId)) {
                    shop.suspiciousUsers.put(r.userId, 1);
                } else {
                    Integer count = shop.suspiciousUsers.get(r.userId);
                    shop.suspiciousUsers.put(r.userId, count + 1);
                }
            }
        }
        return shopListCopy.values();
    }

    /**
     * Calculate the concentration of last hour (time indicated by shop.clock).
     */
    private static int concentration(Shop shop) {
        HashSet<Long> users = new HashSet<>();
        final Queue<Order> recentOrders = shop.recentOrders;
        final Date clock = shop.clock;
        assert clock != null;
        int transactionThisHour = 0;
        for (Order r : recentOrders) {
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
     * make a deep copy of ShopList, used for query
     */
    public final ShopList deepCopy() {
        ShopList shopListCopy = new ShopList(window, concentrationThreshold, increment);
        for (Shop shop : shopList.values()) {
            shopListCopy.shopList.put(shop.shopId, shop.clone());
        }
        return shopListCopy;
    }

    long getWindow() {
        return window;
    }

    long getIncrement() {
        return increment;
    }

    int getConcentrationThreshold() {
        return concentrationThreshold;
    }

    void setWindow(long window) {
        this.window = window;
    }

    void setIncrement(long increment) {
        this.increment = increment;
    }

    void setConcentrationThreshold(int concentrationThreshold) {
        this.concentrationThreshold = concentrationThreshold;
    }
}
