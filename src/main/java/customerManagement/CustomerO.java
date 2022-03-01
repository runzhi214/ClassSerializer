package customerManagement;

public class CustomerO {
    private final int customerID;
    private final String customerName;

    public CustomerO(int ID,String Name){
        customerID = ID;
        customerName = Name;
    }

    public int getID(){
        return customerID;
    }
    public String getName(){
        return customerName;
    }
}
