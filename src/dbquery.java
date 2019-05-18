
public class dbquery {
    public static void main(String[] args) throws Exception {
            if (args.length!=2){  //check parameter
                System.err.println("Please correct parameter");
                return;
            }

            String txt = args[0];
            int pageSize = Integer.parseInt(args[1]);

            CreateHeap h = new CreateHeap(pageSize);

	}

}
