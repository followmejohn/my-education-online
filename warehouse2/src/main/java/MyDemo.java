import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class MyDemo {
    public static void main(String[] args) {
    }
    public int maxProfit(int[] prices) {
        int profit = 0;
        int buy = Integer.MAX_VALUE;
        int sale = 0;
        boolean flag = false;
        ArrayList<Integer> list = new ArrayList<>();
        if(prices.length < 2){
            return 0;
        }
        for(int i = 0; i < prices.length-1; i++){
            if(prices[i] < prices[i + 1]){
                buy = Math.min(buy,prices[i]);
                flag = true;
            }else if(prices[i] > prices[i + 1]){
                if(flag){
                    sale = prices[i];
                    profit = sale - buy;
                    list.add(profit);
                    flag = false;
                    buy = Integer.MAX_VALUE;
                }
            }
        }
        if(flag){
            profit = prices[prices.length - 1] - buy;
            list.add(profit);
        }
        int[] arr = new int[list.size()];
        for (int i = 0; i < list.size(); i++) {
            arr[i] = list.get(i);
        }
        Arrays.sort(arr);
        if(arr.length == 1) return arr[0];
        if(arr.length == 0) return 0;
        return arr[arr.length - 1] + arr[arr.length - 2];
    }

        ArrayList<String> list = new ArrayList<>();
    public static String[] permutation(String S) {
        char[] arr = S.toCharArray();
        ArrayList<char[]> list = new ArrayList<>();
        for(int i = 0; i < arr.length; i++){
            if(i == 0){
                char[] origin = new char[1];
                origin[0] = arr[0];
                list.add(origin);
            }else {
                ArrayList<char[]> tmp = new ArrayList<>();//创建临时集合
                for(int j = 0; j < list.size(); j++){
                    char[] sb = list.get(j);//从list中取一个
                    for(int k = 0; k < (sb.length + 1); k++){//新插入的字符可以在0~（c.length-1）中的任意位置
                        char[] c = new char[sb.length + 1];//临时数组，长度为取出来的sb加1
                        c[k] = arr[i];//插入
                        int k2 = 0;//数组c的索引
                        int offset = 0;//字符串sb的索引
                        while(k2 < c.length){//按顺序插入剩余字符
                            if(k2 != k){
                                c[k2] = sb[offset++];
                            }
                            k2++;
                        }
                        tmp.add(c);//新生产的字符数组加入临时集合中
                    }
                }
                list = tmp;//将原集合替换成新的
            }
        }
        String[] strs = new String[list.size()];
        for(int of = 0; of < list.size(); of++){
            strs[of] = new String(list.get(of));
        }
        for(String a: strs){
            System.out.println(a);
        }
        return strs;
    }
    public int majorityElement(int[] nums) {
//        HashMap<Integer, Integer> map = new HashMap<>();
//        for(int i = 0; i < nums.length; i++){
//            if(map.containsKey(nums[i])){
//                if((map.get(nums[i]) + 1) > (nums.length / 2)) return nums[i];
//                map.put(nums[i],map.get(nums[i]) + 1);
//            }else {
//                map.put(nums[i], 1);
//            }
//        }
//        return nums[0];
        Arrays.sort(nums);
        return nums[nums.length / 2];
    }
    public static int[] searchRange(int[] nums, int target) {
        int i = Arrays.binarySearch(nums, target);
        if(i >= 0){
            int tmp = i;
            while(i >= 0){
                if(nums[i] == target ){
                    i--;
                }else {
                    break;
                }
            }
            i++;
            while (tmp <= nums.length - 1){
                if(nums[tmp] == target){
                    tmp++;
                }else {
                    break;
                }
            }
            tmp--;
            return new int[] {i,tmp};
        }
        return new int[]{-1,-1};
    }
    //
//    public int nthUglyNumber(int n) {
//        int i = 1;
//        int count = 0;
//        while(count < n){
//            if(isZ(i)){
//                count++;
//            }
//            i++;
//        }
//        i--;
//        return i;
//    }
//    public boolean isZ(int a){
//        if(a == 1) return true;
//        if(a % 2 == 0){
//            return isZ(a / 2);
//        }else if(a % 3 == 0){
//            return isZ(a / 3);
//        }else if(a % 5 == 0){
//            return isZ(a / 5);
//        }else {
//            return false;
//        }
//    }
    public static int nthUglyNumber(int n) {
        if(n == 1){
            return 1;
        }
        int[] nums = new int[n];
        nums[0] = 1;
        int i = 0, j = 0, k = 0;
        int ugly = 1;
        for(int c = 1; c < n ; c++){
            ugly = Math.min(Math.min(nums[i] * 2, nums[j] * 3), nums[k] * 5);
            nums[c] = ugly;
            if(ugly == nums[i] * 2){
                i++;
            }
            if(ugly == nums[j] * 3){
                j++;
            }
            if(ugly == nums[k] * 5){
                k++;
            }
        }
        return ugly;
    }
    //
    public int minimumSwap(String s1, String s2) {
        int xy = 0, yx = 0;
        char[] arr1 = s1.toCharArray();
        char[] arr2 = s2.toCharArray();
        for(int i = 0; i < s1.length(); i++){
            if(arr1[i] == arr2[i]){
                continue;
            }else if(arr1[i] == 'x'){
                xy++;
            }else {
                yx++;
            }
        }
        //如果xy 和yx都是偶数则其中的+1都不起作用即结果为(xy+yx)/2，如果都是奇数则正好等于((xy+yx)/2) + 1;
        return ((xy + yx) & 1) == 1 ? -1: (xy + 1)/ 2 + (yx + 1)/ 2;
    }
    //
    public  int longestSubstring(String s, int k) {
        if(k > s.length() || s.length() == 0) {
            return 0;
        }
        if(s.length() == 1) return 1;
        return count(s.toCharArray(),k, 0, s.length() - 1);

    }
    public int count(char[] arr, int k, int left, int right){
        if(right - left + 1 < k) return 0;
        int[] counts = new int[26];
        for(int i = left; i <= right; i++){
            counts[arr[i] - 'a']++;
        }
        while (right - left + 1 >= k && counts[arr[left] - 'a'] < k ){
            left++;
        }
        while (right - left + 1 >= k && counts[arr[right] - 'a'] < k ){
            right--;
        }
        if(right - left + 1 < k) return 0;
        for(int j = left; j <= right; j++){
            if(counts[arr[j] - 'a'] < k){
                return Math.max(count(arr,k,left,j - 1), count(arr, k, j + 1, right));
            }
        }
        return right - left + 1;

    }
    //
}
class MajorityChecker {
    private int[] arr = null;
    public MajorityChecker(int[] arr) {
        this.arr = arr;
    }

    public int query(int left, int right, int threshold) {
        if(left >=0 && left <= right && right < arr.length && (2 * threshold) > (right - left + 1)){
            int[] tmp = new int[right - left + 1];
            for(int offset = 0; offset < tmp.length; offset++){
                tmp[offset] = arr[left];
                left++;
            }
            Arrays.sort(tmp);
            int a = tmp[(tmp.length - 1)/2];
            int count = 0;
            for(int i: tmp){
                if(i == a){
                    count++;
                }
            }
            return count >= threshold ? a : -1;
        }else {
            return -1;
        }
    }
}
