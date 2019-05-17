public class BPlusTree<K extends Comparable<? super K>, V> {
    
    /**
    * The branching factor for the B+ tree, that measures the capacity of nodes
    * (i.e., the number of children nodes) for internal nodes in the tree.
    */
   private int branchingFactor;

   /**
    * The root node of the B+ tree.
    */
   private Node root;

    public BPlusTree(int branchingFactor) {
        if (branchingFactor <= 2)
            throw new IllegalArgumentException("Illegal branching factor: " + branchingFactor);
        this.branchingFactor = branchingFactor;
        root = new LeafNode();
    }

    public void insert(K key, V value) {
        root.insertValue(key, value);
    }
    
    void writeIndex(int pageSize) {
    }

    private abstract class Node { 

        public Node() {
        }

        private void insertValue(K key, V value) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    }

    private static class LeafNode extends Node {

        public LeafNode() {
        }
    }
    
}
