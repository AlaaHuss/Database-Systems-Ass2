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

    void insert(String key, String valueOf) {
    }

    void writeIndex(int pageSize) {
    }

    private static class Node {

        public Node() {
        }
    }

    private static class LeafNode extends Node {

        public LeafNode() {
        }
    }
    
}
