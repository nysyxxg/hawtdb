package org.fusesource.hawtdb.util;

public class AssertTest {
    public static void main1(String[] args) {
        boolean isSafe = true;
        assert isSafe : "Not safe at all";
        System.out.println("断言通过!");
    }
    
    public static void main2(String[] args) {
        boolean isSafe = false;
        assert 1 > 2;
        System.out.println("断言通过!");
    }
    
    
    public static void main(String[] args) {
        double x =  -10; //可以手动改变x的值，重复运行查看不同的运行结果
        assert x > 0 : "x小于0";// 这里使用了断言，规定x必须大于0，否则会抛出异常，并把“x小于0”作为报错信息(必须要开启断言机制，否则类加载器会跳过这行代码)
        double y = Math.sqrt(x);
        System.out.println(y);
        
    }
    
}
