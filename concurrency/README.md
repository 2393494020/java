# 并发编程
1. 原子性
```
    x = 10;        // 赋值操作，直接将数值10写入到工作内存中，具备原子性
    y = x;         // 先读取x的值，再将x的值写入工作内存，两个动作有先后顺序，不再具备原子性
    x++;           // 读取x的值，进行加1操作，写入新的值，三个动作有先后顺序，不再具备原子性
    x = x + 1;     // 读取x的值，进行加1操作，写入新的值，三个动作有先后顺序，不再具备原子性
```
2. 可见性
3. 有序性

# volatile
https://www.cnblogs.com/dolphin0520/p/3920373.html

# 多线程面试题
http://www.threadworld.cn/archives/99.html


# io
https://blog.csdn.net/baiye_xing/article/details/74331041

# socket
https://blog.csdn.net/weixin_39634961/article/details/80236161

## bio
https://blog.csdn.net/skiof007/article/details/52873421

## nio
https://www.cnblogs.com/geason/p/5774096.html  
https://javadoop.com/post/java-nio  
http://www.importnew.com/26368.html  

## aio
aio(linux 不支持)

## epoll

## selector

## reactor