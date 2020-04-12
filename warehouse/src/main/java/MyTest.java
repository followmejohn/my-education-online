import java.util.Arrays;
import java.util.Hashtable;

public class MyTest {

    public static void main(String[] args) {
        String arr = "nihaoa";
        String s1 = "asd1kfj1";
        String s = arr.replaceAll("h","i");
        StringBuffer sb = new StringBuffer(s);
        System.out.println(sb.reverse());
        String haha = s1.replaceFirst("\\d+", "A");
        System.out.println(haha);
        System.out.println("------------------");
        System.out.println(s);
    }
    private DlinkedNode head, tail;
    private Hashtable<Integer,DlinkedNode> cache = new Hashtable<Integer,DlinkedNode>();
    private int capacity;
    private int size;
    class DlinkedNode{
        int key;
        int value;
        DlinkedNode prev;
        DlinkedNode next;
    }
    private void addNode(DlinkedNode node){
        if (node == null) return;
        node.next = head.next;
        node.prev = head;
        head.next = node;
        head.next.prev = node;
    }
    private void deleteNode(DlinkedNode node){
        if (node == null) return;
        node.next.prev = node.prev;
        node.prev.next = node.next;
    }
    private DlinkedNode deleteTail(){
        if (tail.prev == head ) return null;
        DlinkedNode t = tail.prev;
        deleteNode(t);
        return t;
    }
    private void moveToHead(DlinkedNode node){
        if (node == null) return;
        deleteNode(node);
        addNode(node);
    }
    public MyTest(int capacity){
        this.size = 0;
        this.capacity = capacity;
        head = new DlinkedNode();
        tail = new DlinkedNode();
        head.next = tail;
        tail.prev = head;
    }
    public int get(int key){
        DlinkedNode node = cache.get(key);
        if (node == null) return -1;
        moveToHead(node);
        return node.value;
    }
    public void put(int key, int value){
        DlinkedNode node = new DlinkedNode();
        if(cache.get(key) == null){
            node.key = key;
            node.value = value;
            addNode(node);
            cache.put(key, node);
            ++size;
            if (size > capacity){
                DlinkedNode t = deleteTail();
                cache.remove(t.key);
                --size;
            }
        }else {
            node.value = value;
            moveToHead(node);
        }
    }
}





















