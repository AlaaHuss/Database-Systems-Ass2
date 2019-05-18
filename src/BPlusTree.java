
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

    private class InternalNode extends Node {
        List<Node> children;

        InternalNode() {
            this.keys = new ArrayList<K>();
            this.children = new ArrayList<Node>();
        }

        @Override
        void insertValue(K key, V value) {
            Node child = getChild(key);
            child.insertValue(key, value);
            if (child.isOverflow()) {
                Node sibling = child.split();
                insertChild(sibling.getFirstLeafKey(), sibling);
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

        @Override
        boolean isOverflow() {
            return children.size() > branchingFactor;
        }

        @Override
        Node split() {
            int from = keyNumber() / 2 + 1, to = keyNumber();
            InternalNode sibling = new InternalNode();
            sibling.keys.addAll(keys.subList(from, to));
            sibling.children.addAll(children.subList(from, to + 1));

            keys.subList(from - 1, to).clear();
            children.subList(from, to + 1).clear();

            return sibling;
        }

        @Override
        K getFirstLeafKey() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
        Node getChild(K key) {
            int loc = Collections.binarySearch(keys, key);
            int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
            return children.get(childIndex);
        }
        
        void insertChild(K key, Node child) {
            int loc = Collections.binarySearch(keys, key);
            int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
            if (loc >= 0) {
                    children.set(childIndex, child);
            } else {
                    keys.add(childIndex, key);
                    children.add(childIndex + 1, child);
            }
        }
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
        
        @Override
        Node split() {
            LeafNode sibling = new LeafNode();
            int from = (keyNumber() + 1) / 2, to = keyNumber();
            sibling.keys.addAll(keys.subList(from, to));
            sibling.values.addAll(values.subList(from, to));

            keys.subList(from, to).clear();
            values.subList(from, to).clear();

            sibling.next = next;
            next = sibling;
            return sibling;
        }
        
        @Override
        boolean isOverflow() {
            return values.size() > branchingFactor - 1;
        }
        
        @Override
        K getFirstLeafKey() {
            return keys.get(0);
        }
    }
    
    private abstract class Node { 
        List<K> keys;
        
        int keyNumber() {
            return keys.size();
        }

        abstract void insertValue(K key, V value);
        abstract boolean isOverflow();
        abstract Node split();
        abstract K getFirstLeafKey();
    }
    
}
