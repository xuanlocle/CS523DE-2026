import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StationTempKey implements WritableComparable<StationTempKey> {

    private Text stationId = new Text();
    private IntWritable maxTemp = new IntWritable();

    public StationTempKey() {}

    public StationTempKey(String stationId, int temp) {
        this.stationId.set(stationId);
        this.maxTemp.set(temp);
    }

    public Text getStationId() {
        return stationId;
    }

    public IntWritable getMaxTemp() {
        return maxTemp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        stationId.write(out);
        maxTemp.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        stationId.readFields(in);
        maxTemp.readFields(in);
    }

    @Override
    public int compareTo(StationTempKey other) {

        int cmp = stationId.compareTo(other.stationId);

        if (cmp != 0)
            return cmp;

        // Temperature DESC
        return -1 * maxTemp.compareTo(other.maxTemp);
    }
}
