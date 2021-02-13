package max.learn.hdfs.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 *
 * 使用 MR 统计 “本地" HDFS 上的文件对应的词频
 *
 */
public class WordCountLocalApp {

    // avoid error in Windows, load dll manually
    static {
        try {
            System.load("D:/hadoop-2.7.1/bin/hadoop.dll");
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception{

        Logger logger = Logger.getLogger(WordCountLocalApp.class);

        Configuration configuration = new Configuration();

        // 如果输出目录已经存在，则先删除
        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path("data/wordcount/output");
        if(fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath,true);
        }
        fileSystem.close();

        // 创建一个Job
        Job job = Job.getInstance(configuration);

        // 设置Job对应的参数: 主类
        job.setJarByClass(WordCountLocalApp.class);

        // 设置Job对应的参数: 设置自定义的Mapper和Reducer处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置Job对应的参数: Mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Job对应的参数: Reduce输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置Job对应的参数: Mapper输出key和value的类型：作业输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path("data/wordcount/input/hello.txt"));
        FileOutputFormat.setOutputPath(job, new Path("data/wordcount/output"));

        // 提交job
        boolean result = job.waitForCompletion(true);

        logger.info("this is a test log");

        System.out.println("DONE, check output at: data/wordcount/output");
        System.exit(result ? 0 : -1);

    }
}
