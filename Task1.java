package bigdata;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
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


public class Task1 extends Configured implements Tool {
	
	public static void main(String args[])throws Exception{
		ToolRunner.run(new Task1(), args);
	}
	 
	public int run(String[] args) throws Exception {
		String input = args[0];		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(Task1.class);
		job.setMapperClass(Task1Map.class);
		job.setReducerClass(Task1Reduce.class);		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);			
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0])); //입력 파일을 받음 
		FileOutputFormat.setOutputPath(job, new Path("Task1.txt")); //출력은 task1.txt로 저장한다. 
		
		job.waitForCompletion(true);
		return 0;
	}
	//map function
	public static class Task1Map extends Mapper<Object, Text, IntWritable, IntWritable>{
		IntWritable ou = new IntWritable(); //Intwritable ou 선언 
		IntWritable ov = new IntWritable(); //Intwritable ov 선언 
		@Override
		protected void map(Object key, Text value, Mapper<Object, 
				Text, IntWritable, IntWritable>.Context context)
						throws IOException, InterruptedException { 
			StringTokenizer st = new StringTokenizer(value.toString());
			//라인별로 읽기 실행 
			ou.set(Integer.parseInt(st.nextToken())); //첫번째 vertx : ou  
			ov.set(Integer.parseInt(st.nextToken())); //두번째 vertex : ov
			if(ou.get() < ov.get()) context.write(ou, ov); //ou < ov라면 (ou,ov)기록 
			else if(ou.get() > ov.get())context.write(ov, ou); //ou > ov라면 (ov,ou)기록 	
			//loop는  조건문 조건에 의하여 map함수에 기록되지 않는다 => loop들은 제거됨 (ou=ov 이기 때문)  
			//(key,value)에서 key값은 value 값보다 무조건 작아진다. 이를 통하여 중복들을 제거할수 있다
			// ex) (3,4)는 들어가지나 (4,3)은 (3,4)로 바뀌어서 들어가짐. 즉 중복이 제거됨 
		}
	}
	//map의 combiner에 의하여 key,(value,value,......)의 key,values꼴로 reduce함수에 전달됨
	
	//reduce function 
	public static class Task1Reduce extends 
	Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		IntWritable ok = new IntWritable(); //Intwritable ok 선언 
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			Set<Integer> a = new HashSet<Integer>(); //hashset 사용해주기(즉 같은 key값에 대하여 같은 value들은 들어가지지 않는다.(중복제거))			
			for(IntWritable v : values) {
				a.add(v.get()); //values들을 hashset에 집어넣어주기 
			}						
			for(int i : a) { 
				ok.set(i); //key기준으로(한 vertex기준) 연결된 vertex(values)들을 ok에 설정해주고 
				context.write(key,ok);//(key,ok)출력해주기
			}
			
		}
	}
	
	
	
	
	
}
