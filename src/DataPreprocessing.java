import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Scanner;

import orderBrushing.DetectOrderBrushing;
import orderBrushing.Order;

/**
 * Sort the events and write to new file (only need to be processed once). The
 * purpose of the sorting is to simulate actual transaction scene, since actual
 * transactions happen in time order.
 */
public final class DataPreprocessing {
    public static void main(String[] args) {
        final File table = new File("data/order_brush_order.csv");
        final File orderedOrder = new File("data/ordered_order.csv");
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try {

            // read original data
            Scanner scanner = new Scanner(table);
            ArrayList<Order> orders = new ArrayList<>();
            scanner.nextLine();
            while (scanner.hasNext()) {
                Order order = DetectOrderBrushing.parseLine(scanner.nextLine(), dateFormat);
                orders.add(order);
            }
            scanner.close();
            orders.sort(Order.TIME_COMPARATOR);

            // write into ordered log file
            FileWriter fileWriter = new FileWriter(orderedOrder);
            fileWriter.write("orderid,shopid,userid,event_time\n");
            for (Order order : orders) {
                fileWriter.append(String.format("%d,%d,%d,%s\n", order.orderId, order.shopId, order.userId,
                        dateFormat.format(order.eventTime)));
            }

            // mark the end of input stream
            fileWriter.close();

        } catch (IOException e) {
            System.out.println("Data not found!");
        } catch (ParseException e2) {
            e2.printStackTrace();
        }
    }
}
