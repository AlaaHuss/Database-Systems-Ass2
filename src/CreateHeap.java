
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;


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
        String line = bi.readLine();
        int rec_num = 0;
        while (line != null) {

            String[] recordFields = line.split(",");
            String key = recordFields[0] + " " + recordFields[1];
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
        long finish = System.currentTimeMillis();
        long pages = (long) Math.ceil(((double) totalFiles * (double) recordSize) / (double) pageSize);
        System.out.println("Pages = " + (pages));
        System.out.println("Records = " + totalFiles);
        System.out.println("Time (ms) = " + (finish - start));
        out.close();

    }

    private byte[] createRecord(String[] recordFields) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private void writePage(DataOutputStream out, byte[] page) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
