package statistic;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class CommentStatistic {

	/**
	 * @param args
	 */
	static JavaSparkContext sc = null;
	static JavaRDD<String> inputRDD = null;
	static JavaRDD<String[]> splitRDD = null;
	
	public CommentStatistic(String txtPath){
		this.sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("count"));
		this.inputRDD = sc.textFile(txtPath);		
		this.splitRDD = inputRDD.map(new splitRecord()).filter(new dataClean());
	}
	
	  public static void avgScore(){
		//日平均分：时间：平均分	
		JavaPairRDD<String, String> timeScoreRDD = splitRDD.mapToPair(new createPair(0));// time: score
		JavaPairRDD<String, AvgCount> timeScoreIteratorRDD = timeScoreRDD.filter( new filterEmptyScore()).combineByKey(createAcc, addAndCount, combine);
		Map<String, AvgCount> countMap = timeScoreIteratorRDD.collectAsMap();
		for(Entry<String, AvgCount> entry: countMap.entrySet()){
			System.out.println(entry.getKey() + " 的当日平均分: " + entry.getValue().avg());
		}	

		}
	  public static void avgCom (){
		  //日评论量：时间：评论量
		JavaPairRDD<String, String> timeCommentRDD = splitRDD.mapToPair(new createPair(2));// time: comment
		JavaPairRDD<String, Iterable<String>> timeComIterator = timeCommentRDD.filter(new filterEmptyComment()).groupByKey();
		Map<String, Iterable<String>> commMap = timeComIterator.collectAsMap();		
		for(Entry<String, Iterable<String>> entry2: commMap.entrySet()){
			int length =0;
			Iterable<String> entryValue = entry2.getValue();
			for(String comment : entryValue){
				length += 1;
			}
			System.out.println(entry2.getKey() + "  的评论量: " + length);
		}		  
	  }
	  static class filterEmptyComment implements Function<Tuple2<String, String>, Boolean>{
		  //清除掉空的comment
		 public Boolean call(Tuple2<String, String> tup){
			 if(tup._2.equals(""))
				 return false;
			 else return true;
		 }
	 };
	  
	public static class createPair implements PairFunction<String[], String, String>{
		//创建键值对，取下标时间为键，下标 lastIndex为值
		//int firstIndex;
		int lastIndex ;
		createPair( int second){
			//this.firstIndex = first;
			this.lastIndex = second;
		}
		public Tuple2<String, String> call(String[] s){
			return new Tuple2<String, String>(s[1].substring(0, 10), s[this.lastIndex]);
		}
	}
	
	public static class splitRecord implements Function<String, String[]>{
		//把一条记录切分
		public String[] call(String s){
			String[] result = s.split(",");
			return result;
		}
	}
	public static class dataClean implements Function<String[], Boolean>{
		//数据清洗，切分后数组长度为3
		public Boolean call(String[] s){
			if(s.length == 3)
				return true;
			else return false;
		}
	}

	  static class filterEmptyScore implements Function<Tuple2<String, String>, Boolean>{
		  //清除掉空的分数和非数字
		 public Boolean call(Tuple2<String, String> tup){
			   Pattern pattern = Pattern.compile("[0-9]*"); 
			   Matcher isNum = pattern.matcher(tup._2);
			 if(tup._2.equals("") || !isNum.matches())
				 return false;
			 else return true;
		 }
	 };
	  
		 static class AvgCount implements Serializable{
			double total_;
			double num_;
			AvgCount(double total, double num){
				this.total_ = total;
				this.num_ = num;
			}
			float avg(){
				return (float) (this.total_ / this.num_);
			}
		 };
		
		static Function<String, AvgCount> createAcc = new Function<String, AvgCount>(){
			 public AvgCount call(String x){
				return new AvgCount(Double.parseDouble(x), 1);
			}
		};
		
		static Function2<AvgCount, String, AvgCount> addAndCount = 
				new Function2<AvgCount, String, AvgCount>(){
			 public AvgCount call(AvgCount a, String x){
				a.total_ += Double.parseDouble(x);
				a.num_ += 1;
				return a;
			}
		};
		
		static Function2<AvgCount, AvgCount, AvgCount> combine = 
				new Function2<AvgCount, AvgCount, AvgCount>(){
			 public AvgCount call(AvgCount a, AvgCount b){
				a.total_ += b.total_;
				a.num_ += b.num_;
				return a;
			}
		};
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CommentStatistic test = new CommentStatistic("/home/iiip/idFile/564ef2c1c72307ab5599ddc5.txt");
		test.avgScore();
		test.avgCom();
	}
}
