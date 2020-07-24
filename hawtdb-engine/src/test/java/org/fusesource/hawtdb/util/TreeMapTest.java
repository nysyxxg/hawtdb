package org.fusesource.hawtdb.util;

import java.util.Comparator;
import java.util.Iterator;


public class TreeMapTest {
    
    public static void main(String[] args) {
        //初始化自定义比较器
        Comparators.MyComparator comparator = new Comparators.MyComparator();
        //初始化一个map集合
        TreeMap<String,String> map = new TreeMap<String,String>(comparator);
        //存入数据
        map.put("a", "a");
        map.put("b", "b");
        map.put("f", "f");
        map.put("d", "d");
        map.put("c", "c");
        map.put("g", "g");
        //遍历输出
        Iterator iterator = map.keySet().iterator();
        while(iterator.hasNext()){
            String key = (String)iterator.next();
            System.out.println(map.get(key));
        }
        System.out.println("---------------------------");
        TreeMap.TreeEntry  firstEntry =  map.firstEntry();
        System.out.println("-----------------firstEntry----------"+ firstEntry);
    
        TreeMap.TreeEntry  lastEntry =  map.lastEntry();
        System.out.println("-----------------lastEntry----------"+ lastEntry);
    
        TreeMap.TreeEntry  ceilingEntry =  map.ceilingEntry("f");
        System.out.println("-----------------ceilingEntry----------"+ ceilingEntry);
    
        TreeMap.TreeEntry  floorEntry =  map.floorEntry("f");
        System.out.println("-----------------floorEntry----------"+ floorEntry);
    }
    
}
