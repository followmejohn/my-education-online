import org.apache.calcite.util.mapping.Mappings;
import org.apache.spark.sql.sources.In;
import scala.Int;

import java.util.*;

public class MyTest2 {
    public static void main(String[] args) {
        Node node = new Node(7);
        node.random = null;
        Node node1 = new Node(13);
        node1.random = node;
        node.next = node1;
        System.out.println(copyRandomList(node));
    }
    public List<String> topKFrequent(String[] words, int k) {
        Map<String, Integer> map = new HashMap<>();
        for(String word: words){
            map.put(word,map.getOrDefault(word,0) + 1);
        }
        Set<String> strings = map.keySet();
        List<String> list = new ArrayList<>(strings);
        Collections.sort(list, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                if(map.get(o1) == map.get(o2)){
                    return o1.compareTo(o2);
                }
                return map.get(o2) - map.get(o1);
            }
        });
        return list.subList(0,k);
    }
    //这中递归方法在数据大时会超时（leetcode1276 汉堡不浪费问题）
    int count1 = 0, count2 = 0;
    public List<Integer> numOfBurgers(int tomatoSlices, int cheeseSlices) {
        List<Integer> list = new ArrayList<>();
        if(isOK(tomatoSlices,cheeseSlices,0,0)){
            list.add(count1);
            list.add(count2);
            return list;
        }else {
            return list;
        }
    }
    public boolean isOK(int tomatoSlices, int cheeseSlices, int ct1, int ct2){
        if(tomatoSlices == 0 && cheeseSlices == 0) {
            count1 = ct1;
            count2 = ct2;
            return true;
        }
        if(tomatoSlices <= 0 || cheeseSlices <= 0) return false;
        return isOK(tomatoSlices - 4,cheeseSlices -1,ct1+1, ct2) ||
                isOK(tomatoSlices - 2, cheeseSlices -1,ct1,ct2+1);
    }
    //
    public List<Integer> numOfBurgers2(int tomatoSlices, int cheeseSlices) {
        List<Integer> list = new ArrayList<>();
        int diff = tomatoSlices - cheeseSlices;
        if(diff % (4 -2) == 0 && tomatoSlices < 4*cheeseSlices && diff >= 0){
            int tmp = diff/(4-2);
            list.add(tmp);
            list.add(cheeseSlices - tmp);
            return list;
        }else {
            return list;
        }
    }
    //
    public boolean isSubtree(TreeNode s, TreeNode t) {
        if(s == null && t == null) return true;
        if(s == null || t == null) return false;
        if(s.val == t.val && isTheSame(s.left,t.left) && isTheSame(s.right,t.right)){
            return true;
        }else {
            return isSubtree(s.left,t) || isSubtree(s.right, t);
        }
    }
    //判断两颗树是否相同
    public boolean isTheSame(TreeNode t1,TreeNode t2){
        if(t1 == null && t2 == null) return true;
        if(t1 == null || t2 == null) return false;
        if (t1.val == t2.val){
            return isTheSame(t1.left, t2.left) && isTheSame(t1.right, t2.right);
        }else {
            return false;
        }
    }
    public int minDiffInBST(TreeNode root) {
        List<Integer> list = new ArrayList<>();
        ct(root, list);
        Collections.sort(list);
        return list.get(0);
    }
    public void ct(TreeNode root, List<Integer> list){
        if(root == null) return;
        if(root.left != null){
            list.add(root.val - root.left.val);
            if(root.left.left != null || root.left.right != null) ct(root.left,list);
        }
        if(root.right != null){
            list.add(root.right.val - root.val);
            if(root.right.left != null || root.right.right != null) ct(root.right,list);
        }
    }
    //
    public int findUnsortedSubarray(int[] nums) {
        int[] arr = new int[nums.length];
        for(int i = 0; i < nums.length; i++){
            arr[i] = nums[i];
        }
        Arrays.sort(arr);
        int a1 = 0;
        int a2 = arr.length - 1;
        while (a1 < a2){
            if(arr[a1] != nums[a1] && arr[a2] != nums[a2]){
                break;
            }
            if(arr[a1] == nums[a1]){
                a1++;
            }
            if(arr[a2] == nums[a2]){
                a2--;
            }
        }
        if(a2 - a1 == 0) return 0;
        return a2 - a1 + 1;
    }
    public static Node copyRandomList(Node head) {
        List<Node> list1 = new ArrayList<>();//存旧链
        List<Node> list2 = new ArrayList<>();//存新链
        if(head == null) return null;
        //处理旧链
        Node old = head;//指向旧链
        Map<Node, Integer> map = new HashMap<>();//用于保存node与数组offset间的关系
        int len = 0;
        while(old != null){ //旧链存进数组
            list1.add(old);
            map.put(old,len);//这里的len数值上可以当做offset
            old = old.next;
            len++;//记录链的长度
        }
        old = head;
        //处理新链
        for(int i = 0; i < len; i++){ //初始化新链数组
            Node node = new Node(0);
            list2.add(node);
        }
        Node newHead = list2.get(0);
        Node now = newHead; // 指向新链
        while(old != null){
            now.val = old.val;
            if(old.next != null){
                now.next = list2.get(map.get(old.next));
            }
            if(old.random != null){
                now.random = list2.get(map.get(old.random));
            }
            old = old.next;
            now = now.next;
        }
        return list2.get(0);
    }

}
class TreeNode {
      int val;
      TreeNode left;
      TreeNode right;
      TreeNode(int x) { val = x; }
  }
class Node {
    int val;
    Node next;
    Node random;

    public Node(int val) {
        this.val = val;
        this.next = null;
        this.random = null;
    }
}