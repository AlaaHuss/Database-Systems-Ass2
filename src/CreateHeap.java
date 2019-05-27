
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;


class CreateHeap {
    int pageSize;
    int recordSize = 197;

    int[] sizes = new int[] {4,22,22,4,6,40,15,4,23,22,26,4,5};
    
    public CreateHeap(int pageSize) {
        this.pageSize = pageSize;
    }

    public void save(String dataFile) throws Exception {

        File file = new File(dataFile);
        
        int recordsPerPage = pageSize / recordSize;

        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("heap." + pageSize)));
        
        BufferedReader bi = new BufferedReader(new InputStreamReader(new FileInputStream(dataFile)));
        bi.readLine();
        
        int currentFile = 0;
        int totalFiles = 0;
        byte[] page = new byte[pageSize];
        long start = System.currentTimeMillis();
        BPlusTree<String, String> bpt = new BPlusTree<String, String>(3);
        String line = bi.readLine();
        int tree_no = 0;
        while (line != null) {

            String[] recordFields = line.split(",");
            String key = recordFields[0] + " " + recordFields[1];
            bpt.insert(key,String.valueOf(totalFiles));
            if(totalFiles != 0 && totalFiles % 500000 == 0){
                bpt.writeIndex(pageSize, tree_no);
                tree_no++;
                bpt = null;
                bpt = new BPlusTree<String, String>(3);
            }
            totalFiles++;
            byte[] record = createRecord(recordFields);

            int recordCursor = currentFile * recordSize;
            // write record to page
            for (int x = 0; x < record.length; x++) {
                page[recordCursor + x] = record[x];
            }
            currentFile++;

            line = bi.readLine();

            if (currentFile == recordsPerPage || line == null) {
                    writePage(out, page);
                currentFile = 0;
            }

        }
        bpt.writeIndex(pageSize, tree_no);
        long finish = System.currentTimeMillis();
        long pages = (long) Math.ceil(((double) totalFiles * (double) recordSize) / (double) pageSize);
        System.out.println("Pages = " + (pages));
        System.out.println("Records = " + totalFiles);
        System.out.println("Time (ms) = " + (finish - start));
        out.close();

    }

    private byte[] createRecord(String[] data) {
        byte[] record = new byte[recordSize];
        int fieldCursor = 0;
        for (int x = 0; x < data.length; x++) {
            if (x == 0 || x == 3 || x == 7 || x == 11) {
                byte[] num = new byte[]{0, 0, 0, 0};
                try {
                    num = ByteBuffer.allocate(4).putInt(Integer.parseInt(data[x])).array();
                } catch (Exception e) {
                }
                for (int y = fieldCursor; y < num.length + fieldCursor; y++) {
                    record[y] = num[y - fieldCursor];
                }
            } else { // non integer
                byte[] bData = data[x].getBytes(StandardCharsets.UTF_8);
                for (int y = fieldCursor; y < bData.length + fieldCursor; y++) {
                    record[y] = bData[y - fieldCursor];
                }
            }
            fieldCursor += sizes[x];
        }
        return record;
    }

    private void writePage(DataOutputStream out, byte[] page) throws Exception {
        out.write(page);
        page = new byte[pageSize];
    }
    /**
        index file
        {children index(line)#[keys]#value(line of heap file)}
        {1#[18443 06/12/2017 11:31:58 AM, 19563 10/11/2017 03:31:27 PM]#4,17}
        {2#[17988 08/28/2017 12:39:48 PM]#9___3#[18972 09/04/2017 01:54:16 PM]#16___4#[20542 03/04/2017 02:51:27 AM]#82}
        {5#[17425 05/08/2017 05:39:12 PM, 17745 07/11/2017 11:34:48 PM]#15,36___6#[18222 03/14/2017 09:41:21 PM]#47}
        {7#[18663 05/31/2017 06:53:26 AM]#495___8#[19206 06/15/2017 01:15:04 PM]#205}
        {9#[19985 11/16/2017 10:38:34 AM]#27___10#[21153 10/21/2017 11:38:40 AM, 21595 07/15/2017 01:18:10 PM]#42,22}
        {11#[17293 09/18/2017 08:29:19 AM]#392___12#[17623 12/17/2017 06:30:00 PM]#192___13#[17854 07/11/2017 06:20:35 PM]#11}
        ...............................................
    */
    public void readFile(String txt) throws FileNotFoundException, IOException {
        int index = 0;
        int subindex = 0;
        String line = "";
        long start = System.currentTimeMillis();
        boolean search_done = false;
        int result = -1;
        
        int resultcnt = 0;
        for(int pg = 0; pg < 10; pg++){     //index file 0 - 9
            LineNumberReader reader = new LineNumberReader(new FileReader(new File("index" + pg + "." + pageSize)));//read a index file
            while ((reader.readLine()) != null);
            int countofline = reader.getLineNumber();   //get line of index file
            while(!search_done){                        
                try {
                    Stream<String> lines = Files.lines(Paths.get("index" + pg + "." + String.valueOf(pageSize)));//read a index file
                    line = lines.skip(index).findFirst().get();         //read index line of index file
                    line = line.substring(1, line.length() - 1);        //get line without {} like this {1#[18443 06/12/2017 11:31:58 AM, 19563 10/11/2017 03:31:27 PM]#4,17} - > 1#[18443 06/12/2017 11:31:58 AM, 19563 10/11/2017 03:31:27 PM]#4,17
                    String subline = "";                                
                    if(line.indexOf("___") > 0){                        //if the child node has more than two nodes
                        subline = line.split("___")[subindex];          //to get child node
                    }else{
                        subline = line;                                 //to get child node
                    }
                    String[] tempNode = subline.split("#");             //to split index of child,keys and value
                    index = Integer.parseInt(tempNode[0]);              //index of child(line of child)
                    if(index > countofline){                            //if index of child > lines of index file
                        search_done = true;                             //end of search
                    }                                                                   
                    String node = tempNode[1].substring(1, tempNode[1].length() - 1);   //get keys
                    if(node.indexOf(", ") > 0){                                     //if there are more than two keys
                        String[] subnode = node.split(", ");
                        if(txt.compareTo(subnode[subnode.length - 1]) > 0){         //if search text > last key    
                            subindex = subnode.length;                              //child node of this is last node in children
                        }else{                                                      //if search text < last key
                            for(int i = 0; i < subnode.length; i++){                //to find child node of this in children
                                if(txt.compareTo(subnode[i]) < 0){              
                                    subindex = i;
                                    break;
                                }
                                if(search_done && txt.equals(subnode[i])){          //if the search text is same with child node 
                                    result = Integer.parseInt(tempNode[2].split(",")[i]);   //result is value(line of heap file) of key
                                }
                            }
                        }
                    }else{
                        if(txt.compareTo(node) < 0){                    //if search text < node
                            subindex = 0;                               
                        }else{
                            subindex = 1;
                        }
                        if(search_done && txt.equals(node)){
                            result = Integer.parseInt(tempNode[2]);         //result is value(line of heap file) of key
                        }
                    }
                }catch(Exception e){
                    System.out.println("File Error!!!");
                    System.out.println(e);
                    search_done = true;
                }
            }
            
            int recordsPerPage = pageSize / recordSize;
            if(result == -1 ){                              //if there is no result in this index file
                resultcnt++;
                
            }else{
                try(InputStream heap = new FileInputStream("heap." + pageSize)){        //read heap file

                    int offset = (result / recordsPerPage) * pageSize;              //to get offest in heap file with result of index file 
                    offset += (result % recordsPerPage) * recordSize;
                    heap.skip(offset);                                              //to skip offset
                    int c;
                    for(int ind = 0;ind < 13;ind++){                                // to get result of record in heap file
                        if(ind == 0 || ind == 3 || ind == 7 || ind == 11){
                            String int_con = "";
                            for(int i = 0; i < sizes[ind]; i++){   
                                c = heap.read();
                                if(c==0){
                                    int_con += "00";
                                }else{
                                    if(Integer.toHexString(c).length() == 1){
                                        int_con += "0" + Integer.toHexString(c);
                                    }else{
                                        int_con += Integer.toHexString(c);
                                    }
                                }
                            }
                            System.out.print(Integer.parseInt(int_con,16));
                        }else{
                            for(int i = 0; i < sizes[ind]; i++){
                                c = heap.read();
                               System.out.print(Character.toString((char) c));
                            }
                        }
                        System.out.println("");
                    }
                    System.out.println();
                }catch(FileNotFoundException e){
                    System.out.println("Can not Find File!!!");
                }
            }
            search_done = false;
            index = 0;
            result = -1;
        }   
        if(resultcnt == 10){                                //if there is no result of all index file. 
            System.out.println("There is no results.");
        }
        long end = System.currentTimeMillis();

        System.out.println("\nTime (ms) = " + (end - start));
    }
    
}
