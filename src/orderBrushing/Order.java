package orderBrushing;

import java.util.Comparator;
import java.util.Date;

/**
 * This class represents an order. It includes the following information:
 * orderId, shopId, userId and eventTime, and also provides different
 * comparators. It requires orderId as a unique identifier of Orders.
 */
public final class Order {
    public final long orderId;
    public final long shopId;
    public final long userId;
    public final Date eventTime;
    public static final Comparator<Order> TIME_COMPARATOR = new Comparator<Order>() {
        @Override
        public int compare(Order o1, Order o2) {
            return o1.eventTime.compareTo(o2.eventTime);
        }
    };
    public static final Comparator<Order> TIME_COMPARATOR_REVERSED = new Comparator<Order>() {
        @Override
        public int compare(Order o1, Order o2) {
            return o2.eventTime.compareTo(o1.eventTime);
        }
    };

    public Order(long orderId, long shopId, long userId, Date eventTime) {
        this.orderId = orderId;
        this.shopId = shopId;
        this.userId = userId;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return String.format("order: %d, shop: %d, user: %d, time: %s", orderId, shopId, userId, eventTime);
    }

    /**
     * The {@code equals()} method assumes that orderId is unique.
     */
    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order r = (Order) o;
        if (orderId != r.orderId) return false;
        return true;
    }
}
