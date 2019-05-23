
public class dbquery {
    public static void main(String[] args) throws Exception {
        if(args.length > 0){
            if (args.length!=2){  //check parameter
                System.out.println("Please correct parameter.");
                return;
            }
        }else{
            System.out.println("Please write the parameters.");
        }
        String txt = args[0];
        int pageSize = Integer.parseInt(args[1]);

        CreateHeap h = new CreateHeap(pageSize);

        h.readFile(txt);

    }

}
