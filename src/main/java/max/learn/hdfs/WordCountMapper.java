package max.learn.hdfs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN: Map任务读数据的key类型，offset，是每行数据起始位置的偏移量  Long, e.g. 0
 * VALUEIN:Map任务读数据的value类型，其实就是一行行的字符串  String, e.g. hello world welcome
 *
 * hello world welcome
 * hello welcome
 *
 * KEYOUT: map方法自定义实现输出的key的类型  String, e.g. hello
 * VALUEOUT: map方法自定义实现输出的value的类型  Integer, e.g. 2
 *
 * 词频统计：相同单词的次数   (word,1)
 *
 * Long,String,Integer 是 Java 里面的数据类型
 * LongWritable,Text 是 Hadoop 自定义类型，好处是能快速序列化和反序列化
 *
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

    /**
     * @param key: offset, e.g. 0
     * @param value: String, e.g. hello world welcome
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 把value对应的行数据按照指定的分隔符拆开
        String[] words = value.toString().split(" ");
        for(String word : words) {
            // (hello,1)  (world,1)
            context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }
    }
}
