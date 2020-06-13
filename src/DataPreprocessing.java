import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Scanner;
import orderBrush.OrderBrushOrder;
import orderBrush.Record;

/**
 * sort the events and write to new file (only need to be processed once)
 */
public final class DataPreprocessing {
    public static void main(String[] args) {
        File table = new File("data/order_brush_order.csv");
        File orderedOrder = new File("data/ordered_order.csv");

        try {

            // read original data
            Scanner scanner = new Scanner(table);
            ArrayList<Record> records = new ArrayList<>();
            scanner.nextLine();
            while (scanner.hasNext()) {
                Record record = OrderBrushOrder.parseLine(scanner.nextLine());
                records.add(record);
            }
            scanner.close();
            records.sort(Record.TIME_COMPARATOR);

            // write into ordered log file
            FileWriter fileWriter = new FileWriter(orderedOrder);
            fileWriter.write("orderid,shopid,userid,event_time\n");
            for (Record record : records) {
                fileWriter.append(String.format("%d,%d,%d,%s\n", record.orderId, record.shopId, record.userId,
                        OrderBrushOrder.DATE_FORMAT.format(record.eventTime)));
            }
            records = null;

            // mark the end of input stream
            fileWriter.append("99999999999,999999999,999999999,2050-01-01 00:00:00\n");
            fileWriter.close();

        } catch (IOException e) {
            System.out.println("Data not found!");
        } catch (ParseException e2) {
            e2.printStackTrace();
        }
    }
}
