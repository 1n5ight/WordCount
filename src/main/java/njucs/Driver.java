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