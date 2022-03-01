package customerManagement;

import java.lang.Math;
import customerManagement.avro.Customer;

public class CustomerGenerator {
    private int curID = -1;
    public Customer getNext(){
        String name = "Customer_" + (int) (Math.random() * 1000);
        curID+=1;
        return new Customer(curID,name);
    }

}
