package orderBrushing;

import java.util.Comparator;
import java.util.Date;

/**
 * This class represents an order. It includes the following information:
 * <ul>
 * <li>orderId: {@code long}</li>
 * <li>shopId: {@code long}</li>
 * <li>userId: {@code long}</li>
 * <li>eventTime: {@code java.util.Date}</li>
 * </ul>
 * The class includes a comparator that compares orders based on transaction
 * time.
 * <p>
 * Note: {@code orderId} <b>must be a unique identifier of Orders</b>.
 * </p>
 */
public final class Order {

    public final long orderId;
    public final long shopId;
    public final long userId;
    public final Date eventTime;
    public static final Comparator<Order> TIME_COMPARATOR = Comparator.comparing(o -> o.eventTime);

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
     * The {@code equals()} method assumes that orderId is unique, but it also
     * checks other fields for comparison.
     */
    @Override
    public final boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Order r = (Order) o;
        return orderId == r.orderId && userId == r.userId && shopId == r.shopId && eventTime.equals(r.eventTime);
    }
}
