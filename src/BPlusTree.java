
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

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
    /**
    *   construct of B+ tree.
    */
    public BPlusTree(int branchingFactor) {
        if (branchingFactor <= 2)
            throw new IllegalArgumentException("Illegal branching factor: " + branchingFactor);
        this.branchingFactor = branchingFactor;
        root = new LeafNode();
    }
    /**
    *   insert method of B+ tree.
    */
    public void insert(K key, V value) {
        root.insertValue(key, value);
    }
    
    /**
    *   toString method to get string value of B+ tree.
    */
    public String toString() {
        Queue<List<Node>> queue = new LinkedList<List<Node>>();
        queue.add(Arrays.asList(root));
        StringBuilder sb = new StringBuilder();
        while (!queue.isEmpty()) {
            Queue<List<Node>> nextQueue = new LinkedList<List<Node>>();
            while (!queue.isEmpty()) {
                List<Node> nodes = queue.remove();
                sb.append('{');
                Iterator<Node> it = nodes.iterator();
                while (it.hasNext()) {
                    Node node = it.next();
                    sb.append(node.toString() + node.getValue((K) node.toString().substring(1, node.toString().length()-1)));

                    if (it.hasNext())
                        sb.append(", ");
                    if (node instanceof BPlusTree.InternalNode)
                        nextQueue.add(((InternalNode) node).children);
                }
                sb.append('}');
                if (!queue.isEmpty())
                    sb.append(", ");
                else
                    sb.append('\n');
            }
            queue = nextQueue;
        }

        return sb.toString();
    }
    /**
    *   writeIndex method to make index file of B+ tree.
    */
    public void writeIndex(int pagesize,int tree_no) throws IOException {
        PrintWriter indexoutFile = new PrintWriter(new FileWriter("index" + tree_no + "." + pagesize));
        Queue<List<Node>> queue = new LinkedList<List<Node>>();
        queue.add(Arrays.asList(root));
        int no = 1;
        while (!queue.isEmpty()) {
            Queue<List<Node>> nextQueue = new LinkedList<List<Node>>();
            while (!queue.isEmpty()) {
                List<Node> nodes = queue.remove();
                indexoutFile.write('{');
                Iterator<Node> it = nodes.iterator();
                while (it.hasNext()) {
                        Node node = it.next();
                        if(node.toString().indexOf(",") > 0){
                            String[] temp = node.toString().substring(1, node.toString().length() - 1).split(", ");
                            indexoutFile.write(no + "#" + node.toString() + "#");
                            for(int i = 0;i < temp.length; i++){
                                indexoutFile.write((String) node.getValue((K) temp[i].toString()));
                                if(i != temp.length - 1){
                                    indexoutFile.write(",");
                                }
                            }
                        }else{
                            indexoutFile.write(no + "#" + node.toString() + "#" + node.getValue((K) node.toString().substring(1, node.toString().length()-1)));
                        }
                        no++;
                        if (it.hasNext())
                                indexoutFile.write("___");
                        if (node instanceof BPlusTree.InternalNode)
                                nextQueue.add(((InternalNode) node).children);
                }
                indexoutFile.write('}');
                indexoutFile.write("\n");
            }
            queue = nextQueue;
        }
        indexoutFile.close();
    }

    /**
    *   InternalNode class of B+ tree.
    */
    private class InternalNode extends Node {
        List<Node> children;
        /**
        *   construct of InternalNode class.
        */
        InternalNode() {
            this.keys = new ArrayList<K>();
            this.children = new ArrayList<Node>();
        }
        /**
        *   insertValue method to insert value to internal node in InternalNode class
        */
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
        /**
        *   isOverflow method to check overflow in InternalNode class
        */
        @Override
        boolean isOverflow() {
            return children.size() > branchingFactor;
        }
        /**
        *   split method in InternalNode class
        */
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
        /**
        *   getFirstLeafKey method to get first leaf key in InternalNode class
        */
        @Override
        K getFirstLeafKey() {
            return children.get(0).getFirstLeafKey();
        }
        /**
        *   getChild method to get child with key in InternalNode class
        */
        Node getChild(K key) {
            int loc = Collections.binarySearch(keys, key);
            int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
            return children.get(childIndex);
        }
        /**
        *   insertChild method to insert child in InternalNode class
        */
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
        /**
        *   getValue method to get value with key in InternalNode class
        */
        @Override
        V getValue(K key) {
            return getChild(key).getValue(key);
        }
    }
    /**
    *   LeafNode class of B+ tree
    */
    private class LeafNode extends Node {
        
        List<V> values;
        LeafNode next;
        /**
        *  construct of LeafNode class
        */
        LeafNode() {
            keys = new ArrayList<K>();
            values = new ArrayList<V>();
        }
        /**
        *  insertValue method to insert key and value in LeafNode class
        */
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
        /**
        *  insertValue method to split key in LeafNode class
        */
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
        /**
        *  isOverflow method to check overflow in LeafNode class
        */
        @Override
        boolean isOverflow() {
            return values.size() > branchingFactor - 1;
        }
        /**
        *  getFirstLeafKey method to get first leaf key in LeafNode class
        */
        @Override
        K getFirstLeafKey() {
            return keys.get(0);
        }
        /**
        *  getValue method to get value with key in LeafNode class
        */
        @Override
        V getValue(K key) {
            int loc = Collections.binarySearch(keys, key);
            return loc >= 0 ? values.get(loc) : null;
        }
    }
    /**
    *  Node class is abstract class for b+ tree
    */
    private abstract class Node { 
        List<K> keys;
        
        int keyNumber() {
            return keys.size();
        }

        abstract void insertValue(K key, V value);
        abstract boolean isOverflow();
        abstract Node split();
        abstract K getFirstLeafKey();
        abstract V getValue(K key);
        public String toString() {
            return keys.toString();
        }
    }
    
}
