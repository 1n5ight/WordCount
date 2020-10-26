### 一、运行流程和结果

1.将程序打包成jar包，并将Shakespeare.txt, punctuation.txt, stop-word-list.txt导入hdfs文件系统下

![img](file:///C:\Users\LUZHON~1\AppData\Local\Temp\ksohtml9064\wps1.jpg) 

 

2.运行wordcount部分

` hadoop jar wordcount.jar wordcount -Dwordcount.case.sensitive=false /input /output -skip1 /punctuation.txt -skip2 /stop-word-list.txt~ `

![img](file:///C:\Users\LUZHON~1\AppData\Local\Temp\ksohtml9064\wps2.jpg) 

结果输出在/output/part-r-00000中，这是未排序的，所以下一步是排序

 

3.运行sort部分

`hadoop jar wordcount.jar sort /output/part-r-00000  /output1`

![img](file:///C:\Users\LUZHON~1\AppData\Local\Temp\ksohtml9064\wps3.jpg) 

结果输出在/output1/part-r-00000，如下图

![img](file:///C:\Users\LUZHON~1\AppData\Local\Temp\ksohtml9064\wps4.jpg) 

 

 

4.下图证明是在集群上运行的

![img](file:///C:\Users\LUZHON~1\AppData\Local\Temp\ksohtml9064\wps5.jpg) 

### 二、设计思路

​		在看完作业要求后，我将作业分为两个部分，第一个部分是词频统计，要求有1.忽略大小写，2.忽略标点符号，3.忽略停词，4.忽略数字，5.单词长度>=3；第二个部分是排序，要求有1.从大到小排列，2.只输出前100个。因此，我在项目中写了两个类，分别是WordCount3和Sort。

​		在词频统计上，首先是仿照WordCount2.0，去掉了标点符号，忽略了大小写。去掉数字也很简单，在patternsToSkip中加入0~9即可。单词长度>=3，只需要在map函数中加入对长度的判断。关键是忽略停词。一开始我只是将停词和标点符号做一样的操作，但发现结果输出来的都不是单词，原因是程序直接将单词内和停词相同的部分换成了空字符串，这是不对的。因此我将输入的第一个skip后的文件放入patternsToSkip，第二个放入stopwordsToSkip。

```java
@Override
    public void setup(Context context) throws IOException, InterruptedException {
      conf = context.getConfiguration();
      caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
      // 分别是标点 和 停词
      if (conf.getBoolean("wordcount.skip.patterns", false)) {
        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
        Path patternsPath = new Path(patternsURIs[0].getPath());
        String patternsFileName = patternsPath.getName().toString();
        parseSkipFile(patternsFileName);
      }
      if (conf.getBoolean("wordcount.skip.stopwords", false)) {
        URI[] stopwordsURIs = Job.getInstance(conf).getCacheFiles();
        Path stopwordsPath = new Path(stopwordsURIs[1].getPath());
        String stopwordsFileName = stopwordsPath.getName().toString();
        stopwordSkipFile(stopwordsFileName);
      }
    }
```

然后，对patternsToSkip，我还是将它们替换成""，对于stopwordsToSkip，我在StringTokenizer操作之后，对每个分割好的单词，到stopwordsToSkip中进行查找，如果有就去掉

```java
@Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
      for (String pattern : patternsToSkip) {
        line = line.replaceAll(pattern, "");
      }
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        String tmp = itr.nextToken();
        // 去掉stopwords
        // 长度大于等于3
        if (tmp.length() >= 3 && !stopwordsToSkip.contains(tmp)) {
          word.set(tmp);
          context.write(word, one);
          Counter counter = context.getCounter(CountersEnum.class.getName(), CountersEnum.INPUT_WORDS.toString());
          counter.increment(1);
        }
      }
    }
```

这样就完成了词频统计部分。



​		在排序上，要注意的就是排序是对key排序的，所以一开始要将value和key交换。还有一点是倒序，需要自己设定排序规则。

```java
public static class DescComparator extends WritableComparator {

		protected DescComparator() {
			super(IntWritable.class, true);
		}

		@Override
		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
			return -super.compare(arg0, arg1, arg2, arg3, arg4, arg5);
		}

		@Override
		public int compare(Object a, Object b) {
			return -super.compare(a, b);
		}
	}
```

### 三、所遇问题

1. hadoop mapreduce运行提示找不到文件
   		这个问题一开始很困扰我，明明在hdfs目录下有文件，命令也没有输错，但就是提示找不到。原因是默认路径在系统进行重启后会被清理，所有之前的文件都失效了。我将hdfs下的文件全部删除，再次导入，解决了这个问题。

2. 将标点和停词区分开后，还是出现删除“单词内停词”的情况
           这个是我对java的操作不了解导致的。`URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();`  ，这其中所有的文件都在getCacheFiles()内，接下来如果按原来的循环，还是无法区分两个文件的作用。因此我给定了两个文件的顺序，Job.getInstance(conf).getCacheFiles()中第一个是标点，第二个是停词。

3. 运行jar包找不到主类

   ​        这是我对pom.xml的配置不熟悉导致的，在pom中加入如下一段即可

   ```xml
   <build>
       <finalName>${project.artifactId}</finalName>
   
       <plugins>
           <plugin>
               <groupId>org.apache.maven.plugins</groupId>
               <artifactId>maven-jar-plugin</artifactId>
               <configuration>
                   <archive>
                       <manifest>
                           <mainClass>njucs.Driver</mainClass>
                           <addClasspath>true</addClasspath>
                           <classpathPrefix>lib/</classpathPrefix>
                       </manifest>
                   </archive>
                   <classesDirectory>
                   </classesDirectory>
               </configuration>
           </plugin>
       </plugins>
     </build>
   ```

4. jar包里有两个类时，无法在命令行中指定运行某个类
           我的jar包里有wordcount和sort两个main，但是无法像hadoop示例程序那样，可以使用`hadoop jar <名称>.jar <指定运行某个类的参数>`  在命令行中指定运行具体的某个类。
           解决方法是我新建了一个类Driver

   ```java
   package njucs;
   
   import org.apache.hadoop.util.ProgramDriver;
   
   public class Driver {
       public static void main(String argv[]) {
           ProgramDriver pgd = new ProgramDriver();
           try {
               pgd.addClass("wordcount", WordCount3.class, "wordcount");
               pgd.addClass("sort", Sort.class, "sort");
               pgd.driver(argv);
           } catch (Throwable e) {
               e.printStackTrace();
           }
       }
   }
   ```


   并将Driver设为主类，这样我就可以在jar包后用wordcount或sort指定运行了。

### 四、可改进之处

1. 两个skip文件的位置不能改动。
   因为我在区别两个文件的时候，指定了它们的位置。预想的解决方法是用-skip1和-skip2来区别，但是由于对java和对hadoop里类的方法不熟悉，没有能实现这一改进
2. wordcount和sort可以合并成一个操作
   我的程序需要两个类，但可以在一个类中定义两个job，分别执行两个mapreduce。但是这个难点在于，第一个词频统计输出的内容在第二步难以定位，如果指定输出的文件夹名称（如output）则只能运行一次，在下次运行时还必须先手动删除output。也不能自动删除output，因为有可能output里有其他重要文件。预想的解决方案是在程序中直接获取第一个mapreduce的输出。