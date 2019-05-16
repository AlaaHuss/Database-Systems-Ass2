
public class dbload {
    public static void main(String[] args) throws Exception {
        if(args.length > 0){
            if (args.length!=3){
                System.out.println("You should input parameters like this. -p <HeapSize> <SourceFile>");
                return;
            }else if(args[0].equals("-p")) {
                int pageSize = Integer.parseInt(args[1]);;
                String fileName = args[2];
                CreateHeap h = new CreateHeap(pageSize);
                h.save(fileName);
            }else{
                System.out.println("You should input correct parameter.");
                return;
            }
        }else{
            System.out.println("You should input parameters like this. -p <HeapSize> <SourceFile>");
            return;
        }
    }
}
