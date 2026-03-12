Step to do:

- Build the jar file: mvn clean package
- Copy the jar file to the Hadoop folder: cp target/StationTemp-1.0-SNAPSHOT.jar /opt/my_code/
- Create input folder in HDFS: hdfs dfs -mkdir -p /user/input
- Upload the input file to HDFS: hdfs dfs -put my_code/NCDC-Weather.txt /user/input
- Run the Hadoop MapReduce job: hadoop jar my_code/StationTemp-1.0-SNAPSHOT.jar StationTempSort /user/input /user/output
- If output folder already exists, remove it: hdfs dfs -rm -r /user/output
- Check the result: hdfs dfs -cat /user/output/part-r-00000
- Save output file to my_code folder: hdfs dfs -get /user/output/part-r-00000 /opt/my_code/