import java.util.Comparator;
import java.util.Date;

/**
 * the data the records a single transaction
 */
public final class Record {
    final long orderId;
    final long shopId;
    final long userId;
    final Date eventTime;
    public static final Comparator<Record> TIME_COMPARATOR = new Comparator<Record>() {
        @Override
        public int compare(Record o1, Record o2) {
            return o1.eventTime.compareTo(o2.eventTime);
        }
    };

    Record(long orderId, long shopId, long userId, Date eventTime) {
        this.orderId = orderId;
        this.shopId = shopId;
        this.userId = userId;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return String.format("order: %d, shop: %d, user: %d, time: %s", orderId, shopId, userId, eventTime);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record r = (Record) o;
        if (orderId != r.orderId || shopId != r.shopId || userId != r.userId || eventTime.compareTo(r.eventTime) != 0) return false;
        return true;
    }
}
