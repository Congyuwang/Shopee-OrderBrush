package orderBrushing;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Queue;

/**
 * The list of shops and the information related to recent transactions
 */
final class ShopList {

    public static final long ONE_HOUR = 1000 * 60 * 60;

    // from shopId to shopInfo
    private final HashMap<Long, Shop> shopList = new HashMap<>();

    /**
     * the main method to update the suspicious list, the orders must be input
     * <em>according to time order</em>.
     *
     * @param order put new transaction here order
     */
    final void update(Order order) {

        // calculate one hour before the transaction time
        final Date oneHourBefore = new Date(order.eventTime.getTime() - ONE_HOUR);

        // initialize if this is a new shop to the list
        if (!shopList.containsKey(order.shopId)) {
            shopList.put(order.shopId, new Shop(order.shopId));
        }
        Shop shop = shopList.get(order.shopId);
        if (shop.clock == null) {
            shop.recentOrders.add(order);
            shop.clock = oneHourBefore;
            return;
        }

        // scan forward to detect high concentration till latest time
        while (shop.clock.compareTo(oneHourBefore) < 0) {
            shop.clock = new Date(shop.clock.getTime() + 1000);
            // fast forward if numberOfOrdersLastHour smaller than 3
            if (detect(shop, order, false) < 3) {
                shop.clock = oneHourBefore;
                break;
            }
        }

        // add new order and detect again
        shop.recentOrders.add(order);
        shop.numberOfOrdersLastHour++;
        detect(shop, order, true);
    }

    /**
     * called when one of the two things happens: either a new order is added into
     * recentOrders, or clock advance by one second.
     */
    private final int detect(Shop shop, Order order, boolean newOrderAdded) {

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

            // calculate numberOfOrdersLastHour
            if (shop.recentOrders.isEmpty() || shop.recentOrders.peek().eventTime.compareTo(shop.clock) >= 0) {
                numberOfOrdersLastHour = shop.recentOrders.size();
            } else {
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
            // recalculation since the concentration does not change, as clock-advance
            // does not insert new orders into recentOrders.
            if (numberOfOrdersLastHour == shop.numberOfOrdersLastHour) {
                return numberOfOrdersLastHour;
            } else {
                // update shop.numberOfOrdersLastHour
                shop.numberOfOrdersLastHour = numberOfOrdersLastHour;
            }
        }

        // calculate concentration of the shop.
        // if the concentration >= 3, enter isPreviousBrushOrder = true period
        if (concentration(shop) >= 3) {
            shop.isPreviousBrushOrder = true;
            return numberOfOrdersLastHour;
        }

        // Else if concentration < 3, but previous concentration > 3, an order-brushing
        // period has just ended, order all suspicious activities into {@code
        // suspiciousTransactionCount} and clear recentOrders.
        if (shop.isPreviousBrushOrder) {
            for (Order r : shop.recentOrders) {
                // skip new order since it is not order-brushing
                if (newOrderAdded && r.equals(order)) {
                    continue;
                }
                // increment suspicious action for corresponding users
                if (!shop.suspiciousUsers.containsKey(r.userId)) {
                    shop.suspiciousUsers.put(r.userId, 1);
                } else {
                    Integer count = shop.suspiciousUsers.get(r.userId);
                    shop.suspiciousUsers.put(r.userId, count + 1);
                }
            }
            shop.recentOrders.clear();
            // add new order back
            if (newOrderAdded)
                shop.recentOrders.add(order);
            // reset isPreviousBrushOrder
            shop.isPreviousBrushOrder = false;
        }

        return numberOfOrdersLastHour;
    }

    /**
     * make a deep copy of shopListCopy, pour all the remaining recent Orders if
     * they are deemed suspicious. Thus it does not disrupt the ShopList main
     * process. Otherwise, it would conduct an early pour.
     *
     * @return a {@code Collection} containing all shop information.
     */
    final Collection<Shop> getShopInfo() {
        // make a deep copy
        HashMap<Long, Shop> shopListCopy = clone().shopList;

        // for each shop
        for (Shop shop : shopListCopy.values()) {
            // if not order brushing, skip.
            if (!shop.isPreviousBrushOrder) {
                continue;
            }
            // if order brushing pour the remaining suspicious transactions.
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
    private static final int concentration(Shop shop) {
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
    @Override
    public final ShopList clone() {
        ShopList shopListCopy = new ShopList();
        for (Shop shop : shopList.values()) {
            shopListCopy.shopList.put(shop.shopId, shop.clone());
        }
        return shopListCopy;
    }
}
