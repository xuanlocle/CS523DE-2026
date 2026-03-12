import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class StationTempSort extends Configured implements Tool {

    public static class SortMapper
            extends Mapper<LongWritable, Text, StationTempKey, IntWritable> {

        private NcdcLineReaderUtils utils = new NcdcLineReaderUtils();
        private StationTempKey outKey = new StationTempKey();
        private IntWritable outValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            utils.parse(value.toString());

            if (utils.isValidTemperature()) {

                String stationId = utils.getStationId();
                int temp = (int) utils.getAirTemperature();
                int year = utils.getYearInt();

                outKey = new StationTempKey(stationId, temp);
                outValue.set(year);

                context.write(outKey, outValue);
            }
        }
    }

    public static class SortReducer
            extends Reducer<StationTempKey, IntWritable, Text, Text> {

        @Override
        protected void reduce(StationTempKey key,
                              Iterable<IntWritable> values,
                              Context context)
                throws IOException, InterruptedException {

            for (IntWritable year : values) {

                String output =
                        key.getStationId().toString() + "\t" +
                                key.getMaxTemp().get() + "\t" +
                                year.get();

                context.write(new Text(output), new Text(""));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: StationTempSort <input> <output>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Station Temp Sort");

        job.setJarByClass(StationTempSort.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(StationTempKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        FileSystem.get(conf).delete(new Path("output"), true);

        int res = ToolRunner.run(conf, new StationTempSort(), args);

        System.exit(res);
    }
}