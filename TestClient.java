import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.Scanner;
import java.util.HashMap;

public class TestClient {

    public static void main(String[] args) {
        File orderedOrder = new File("data/ordered_order.csv");
        File output = new File("output.csv");
        OrderBrushOrder orderBrushOrder = new OrderBrushOrder();

        try {

            // process the records in time order
            FileWriter outputWriter = new FileWriter(output);
            Scanner inputScanner;
            try {
                inputScanner = new Scanner(orderedOrder);
            } catch (IOException e) {
                DataPreprocessing.main(null);
            }
            inputScanner = new Scanner(orderedOrder);
            inputScanner.nextLine();
            while(inputScanner.hasNext()) {
                orderBrushOrder.processNewRecord(inputScanner.nextLine());
            }
            inputScanner.close();

            // request suspiciousShopUser
            HashMap<Long, Long[]> suspiciousShopUser = orderBrushOrder.getSuspiciousShopUser();

            // write the output
            outputWriter.write("shopid,userid\n");
            for (Long shopId : suspiciousShopUser.keySet()) {
                if (shopId == 999_999_999L) {
                    continue;
                }
                outputWriter.append(shopId.toString());
                outputWriter.append(',');
                if (suspiciousShopUser.get(shopId).length == 0) {
                    outputWriter.append('0');
                } else {
                    int count = 0;
                    for (Long userId : suspiciousShopUser.get(shopId)) {
                        if (count++ != 0) outputWriter.append('&');
                        outputWriter.append(userId.toString());
                    }
                }
                outputWriter.append('\n');
            }
            outputWriter.close();
        } catch (IOException e) {
            System.out.println("File not found!");
        } catch (ParseException e2) {
            e2.printStackTrace();
        }
    }
}
