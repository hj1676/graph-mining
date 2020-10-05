package bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Task2 extends Configured implements Tool {

	public static void main(String args[])throws Exception{	
		ToolRunner.run(new Task2(), args);
	}
	

	public int run(String[] args) throws Exception {
		
		String input = args[0];		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(Task2.class);
		job.setMapperClass(Task2Map.class);
		job.setReducerClass(Task2Reduce.class);		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);			
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0])); //입력 파일을 받음
		FileOutputFormat.setOutputPath(job, new Path("Task2.txt")); //출력은 task2.txt로 저장한다.
		
		job.waitForCompletion(true);
		return 0;
	}
	//map function
	public static class Task2Map extends Mapper<Object, Text, IntWritable, IntWritable>{
		IntWritable ou = new IntWritable(); //Intwritable ou 선언
		IntWritable ov = new IntWritable(); //Intwritable ov 선언
		@Override
		protected void map(Object key, Text value, Mapper<Object, 
				Text, IntWritable, IntWritable>.Context context)
						throws IOException, InterruptedException {
			//라인별로 읽기 실행
			StringTokenizer st = new StringTokenizer(value.toString());
			ou.set(Integer.parseInt(st.nextToken())); //첫번째 vertx : ou
			ov.set(Integer.parseInt(st.nextToken())); //두번째 vertex : ov 			
			context.write(ou, ov); //(ou,ov)기록해주기 
			context.write(ov, ou); //(ov,ou)기록해주기  => 즉 한 간선 (a,b)에 대하여 (a,b) (b,a) 두개를 기록해준다 (degree 계산을 위하여) 
		}
	}
	//map의 combiner에 의하여 key,(value,value,......)의 key,values 꼴로 reduce함수에 전달됨 
	
	//reduce function
	public static class Task2Reduce extends 
	Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		IntWritable ok = new IntWritable();	//Intwritable ok 선언	
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			List<Integer> neighbors = new ArrayList<Integer>(); //arraylist 사용해주기 
			for(IntWritable v : values) {
				neighbors.add(v.get()); //arraylist에 모든 value들을 집어넣어줌 
			}
			 ok.set(neighbors.size()); //value들의 개수를 계산함으로서 key(각 정점)마다 degree 계산 가능 
			 						   //ok에 degree수를 집어넣어주기 
			context.write(key, ok); //key와 그 key의 degree수인 ok 같이 출력해주기 
					
			
			
		}
	}
	
	
	
	
	
}
