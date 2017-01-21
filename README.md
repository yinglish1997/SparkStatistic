# SparkStatistic

用spark计算电影日平均分及评论量

每一行原始数据格式如下：
ID,score,time,comment
各个地方可能会有空值。

SplitMovieFile.java 把数据按电影id划分，以ID为名，把每一部电影的其它三项的信息存储为txt文件

CommentStatistic.java 统计。思路是把每一条记录都转化成键值对，如：时间：评分。时间：评论。
-avgScore()求日平均分
-avgCom() 求日评论量
