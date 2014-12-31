package org.p7h.storm.retweets.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.p7h.storm.retweets.utils.RetweetsOrdering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Counts the retweets and displays the retweet info to the console and also logs to the file.
 *
 * @author - Prashanth Babu
 */
public final class RetweetCountBolt extends BaseRichBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetweetCountBolt.class);
    private static final long serialVersionUID = -3110662788756713318L;

    /**
     * Interval between logging the output.
     */
    private final long logIntervalInSeconds;
    /**
     * Log only the retweets which crosses this threshold value.
     */
    private final int rankMaxThreshold;

    private SortedMap<String, Status> retweetCountTracker;
    private long runCounter;
    private Stopwatch stopwatch = null;

    public RetweetCountBolt(final long logIntervalInSeconds, final int rankMaxThreshold) {
        this.logIntervalInSeconds = logIntervalInSeconds;
        this.rankMaxThreshold = rankMaxThreshold;
    }

    @Override
    public final void prepare(final Map map, final TopologyContext topologyContext,
                              final OutputCollector collector) {
        retweetCountTracker = Maps.newTreeMap();
        runCounter = 0;
        stopwatch = Stopwatch.createStarted();
    }

    @Override
    public final void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public final void execute(final Tuple input) {
        final Status retweet = (Status) input.getValueByField("retweet");
        final String screenName = retweet.getUser().getScreenName();
        retweetCountTracker.put(screenName, retweet);

        if (logIntervalInSeconds <= stopwatch.elapsed(TimeUnit.SECONDS)) {
            logRetweetCount();
            stopwatch.reset();
            stopwatch.start();
        }
    }

    private void logRetweetCount() {
        //Doing this circus so that we can sort the Map on the basis of # of retweets a tweet got.
        List<Map.Entry<String, Status>> list = new ArrayList<>(retweetCountTracker.entrySet());
        Collections.sort(list, new RetweetsOrdering());

        if (rankMaxThreshold < list.size()) {
            list = list.subList(0, rankMaxThreshold);
        }

        final List<String> newList = Lists.transform(list, getTweetGist());
        final String retweetInfo = Joiner.on("").join(newList);

        runCounter++;
        LOGGER.info("At {}, total # of retweeted tweets received in run#{}: {}", new Date(),
                runCounter, retweetCountTracker.size());
        LOGGER.info("\n{}", retweetInfo);

        // Empty retweetCountTracker Map for further iterations.
        retweetCountTracker.clear();
    }

    private static Function<Map.Entry<String, Status>, String> getTweetGist() {
        return input -> {
            final Status status = input.getValue();
            final StringBuilder retweetAnalysis = new StringBuilder();
            retweetAnalysis
                    .append("\t")
                    .append(status.getRetweetCount())
                    .append(" ==> @")
                    .append(status.getUser().getScreenName())
                    .append(" | ")
                    .append(status.getId())
                    .append(" | ")
                    .append(status.getText().replaceAll("\n", " "))
                    .append("\n");
            return retweetAnalysis.toString();
        };
    }
}