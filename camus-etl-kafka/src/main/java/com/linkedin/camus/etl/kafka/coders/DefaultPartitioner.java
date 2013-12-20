package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.Partitioner;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

public class DefaultPartitioner implements Partitioner {
    private static Logger log = Logger.getLogger(EtlMultiOutputFormat.class);
    protected DateTimeFormatter outputDateFormatter = null;

    private String getOutputFormat(JobContext context) {
        String granularity = EtlMultiOutputFormat.getOutputPathGranularity(context).toLowerCase();
        if (granularity.equals("hourly")) {
            return "YYYY/MM/dd/HH";
        } else if (granularity.equals("daily")) {
            return "YYYY/MM/dd";
        }else if (granularity.equals("monthly")) {
            return "YYYY/MM";
        } else {
            log.info("Unknown granularity for " + EtlMultiOutputFormat.ETL_OUTPUT_PATH_GRANULARITY + ": " + granularity);
            return "YYYY/MM/dd/HH";
        }
    }

    @Override
    public String encodePartition(JobContext context, IEtlKey key) {
        long outfilePartitionMs = EtlMultiOutputFormat.getEtlOutputFileTimePartitionMins(context) * 60000L;
        return ""+DateUtils.getPartition(outfilePartitionMs, key.getTime());
    }

    @Override
    public String generatePartitionedPath(JobContext context, String topic, int brokerId, int partitionId, String encodedPartition) {
        // We only need to initialize outputDateFormatter with the default timeZone once.
        if (outputDateFormatter == null) {
            outputDateFormatter = DateUtils.getDateTimeFormatter(
                getOutputFormat(context),
                DateTimeZone.forID(EtlMultiOutputFormat.getDefaultTimeZone(context))
            );
        }

        StringBuilder sb = new StringBuilder();
        sb.append(topic).append("/");
        sb.append(EtlMultiOutputFormat.getDestPathTopicSubDir(context)).append("/");
        DateTime bucket = new DateTime(Long.valueOf(encodedPartition));
        sb.append(outputDateFormatter.print(bucket));
        return sb.toString();
    }
}
