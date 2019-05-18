
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


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
        int rec_num = 0;
        while (line != null) {

            String[] recordFields = line.split(",");
            String key = recordFields[0] + " " + recordFields[1];
            bpt.insert(key,String.valueOf(rec_num));
            rec_num++;
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
        bpt.writeIndex(pageSize);
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

    public void readFile(String txt) {
        DataInputStream in = new DataInputStream(new FileInputStream("index." + pageSize));
        int index = 0;
        int subindex = 0;
        String line = "";
        long start = System.currentTimeMillis();
        boolean search_done = false;
        int result = 0;
        LineNumberReader reader = new LineNumberReader(new FileReader(new File("index." + pageSize)));
        while ((reader.readLine()) != null);
        int countofline = reader.getLineNumber();
        
        long end = System.currentTimeMillis();

        System.out.println("\nTime (ms) = " + (end - start));
    }
    
}
