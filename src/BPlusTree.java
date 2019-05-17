
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
        List<K> keys;

        abstract void insertValue(K key, V value);
    }

    private class LeafNode extends Node {
        
        List<V> values;
        LeafNode next;

        LeafNode() {
            keys = new ArrayList<K>();
            values = new ArrayList<V>();
        }

        @Override
        void insertValue(K key, V value) {
            int loc = Collections.binarySearch(keys, key);
            int valueIndex = loc >= 0 ? loc : -loc - 1;
            if (loc >= 0) {
                values.set(valueIndex, value);
            } else {
                keys.add(valueIndex, key);
                values.add(valueIndex, value);
            }
            if (root.isOverflow()) {
                Node sibling = split();
                InternalNode newRoot = new InternalNode();
                newRoot.keys.add(sibling.getFirstLeafKey());
                newRoot.children.add(this);
                newRoot.children.add(sibling);
                root = newRoot;
            }
        }
    }
    
}
