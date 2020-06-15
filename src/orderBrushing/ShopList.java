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
    HashMap<Long, Shop> shopList = new HashMap<>();

    /**
     * the main method to update the suspicious list, the records must be input
     * <em>according to time order</em>.
     *
     * @param record put new transaction here record
     */
    final void update(Record record) {

        // calculate one hour before the transaction time
        final Date oneHourBefore = new Date(record.eventTime.getTime() - ONE_HOUR);

        // initialize if this is a new shop to the list
        if (!shopList.containsKey(record.shopId)) {
            shopList.put(record.shopId, new Shop(record.shopId));
        }
        Shop shop = shopList.get(record.shopId);
        if (shop.clock == null) {
            shop.recentRecords.add(record);
            shop.clock = oneHourBefore;
            return;
        }

        // scan forward to detect high concentration till latest time
        while (shop.clock.compareTo(oneHourBefore) < 0) {
            shop.clock = new Date(shop.clock.getTime() + 1000);
            // fast forward if numberOfOrdersLastHour smaller than 3
            if (detect(shop, record, false) < 3) {
                shop.clock = oneHourBefore;
                break;
            }
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
    private int detect(Shop shop, Record record, boolean newRecordAdded) {

        // if this is a clock advancement event (no new record added):
        // 1. remove unnecessary records from recentRecords.
        // 2. if the orders in the last hour does not change, return.
        int numberOfOrdersLastHour = 0;
        if (!newRecordAdded) {

            // Remove records that are older than one hour if order-brushing is not going
            // on, but keep all records when order-brushing is going on.
            if (!shop.isPreviousBrushOrder) {
                while (!shop.recentRecords.isEmpty() && shop.recentRecords.peek().eventTime.compareTo(shop.clock) < 0) {
                    shop.recentRecords.remove();
                }
            }

            // calculate numberOfOrdersLastHour
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
        // period has just ended, record all suspicious activities into {@code
        // suspiciousTransactionCount} and clear recentRecords.
        if (shop.isPreviousBrushOrder) {
            for (Record r : shop.recentRecords) {
                // skip new record since it is not order-brushing
                if (newRecordAdded && r.equals(record)) {
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
            shop.recentRecords.clear();
            // add new record back
            if (newRecordAdded)
                shop.recentRecords.add(record);
            // reset isPreviousBrushOrder
            shop.isPreviousBrushOrder = false;
        }

        return numberOfOrdersLastHour;
    }

    /**
     * make a deep copy of shopListCopy, pour all the remaining recent Records if
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
            for (Record r : shop.recentRecords) {
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
