package org.p7h.storm.retweets.bolts;

import java.util.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;

/**
 * Counts the retweets and displays the retweet info to the console and also logs to the file.
 *
 * @author - Prashanth Babu
 */
public final class RetweetCountBolt extends BaseRichBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetweetCountBolt.class);
	private static final long serialVersionUID = 2470249820632322227L;
	/** Interval between logging the output. */
    private final long logIntervalInSeconds;
	/** Log only the retweets which crosses this threshold value. */
	private final long logCountThreshold;

	private long lastLoggedTimestamp;
	private Map<String, Status> retweetCountTracker;
	private Multimap<Long, Status> frequencyOfRetweets;
	private long runCounter;

    public RetweetCountBolt(final long logIntervalInSeconds, final long logCountThreshold) {
        this.logIntervalInSeconds = logIntervalInSeconds;
	    this.logCountThreshold = logCountThreshold;
    }

    @Override
    public final void prepare(final Map map, final TopologyContext topologyContext,
                              final OutputCollector collector) {
        lastLoggedTimestamp = System.currentTimeMillis();
	    retweetCountTracker = Maps.newHashMap();
	    //Doing this circus so that the output is in a proper ascending order of the calculated count of retweets.
	    frequencyOfRetweets = Multimaps.newListMultimap(
             new TreeMap<Long, Collection<Status>>(), new Supplier<List<Status>>() {
                 public List<Status> get() {
                     return Lists.newArrayList();
                 }
             }
	    );
    }

    @Override
    public final void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public final void execute(final Tuple input) {
	    final Status retweet = (Status) input.getValueByField("retweet");
	    final String screenName = retweet.getUser().getScreenName();
	    retweetCountTracker.put(screenName, retweet);

	    final long timestampNow = System.currentTimeMillis();
	    final long logPeriodInSeconds = (timestampNow - lastLoggedTimestamp) / 1000;
        if (logPeriodInSeconds > logIntervalInSeconds) {
	        ++runCounter;
	        logRetweetCount();
            lastLoggedTimestamp = timestampNow;
        }
    }

    private final void logRetweetCount() {
	    long count;
	    Status status;
	    //Group retweets based on the Status and the count into a Multimap
	    for (Map.Entry<String, Status> entry : retweetCountTracker.entrySet()) {
		    status = entry.getValue();
	        count = status.getRetweetCount();
		    if (logCountThreshold < count) {
		        frequencyOfRetweets.put(count, status);
		    }
	    }

	    final StringBuilder dumpRetweetInfoToLog = new StringBuilder();

	    Collection<Status> statuses;
	    for (final Long key: frequencyOfRetweets.keySet()) {
		    statuses = frequencyOfRetweets.get(key);
		    for(Status statusToLog: statuses) {
			    dumpRetweetInfoToLog.append(key)
					    .append(" ==> @")
					    .append(statusToLog.getUser().getScreenName())
					    .append(" | ")
					    .append(statusToLog.getId())
					    .append(" | ")
					    .append(statusToLog.getText().replaceAll("\n", " "))
					    .append("\n");
		    }
	    }
	    LOGGER.info("At {}, total # of retweeted tweets received in run#{}: {} ", new Date(),
			               runCounter, retweetCountTracker.size());
	    LOGGER.info("\n{}", dumpRetweetInfoToLog.toString());

	    // Empty frequency and retweetCountTracker Maps for futher iterations.
        retweetCountTracker.clear();
	    frequencyOfRetweets.clear();
    }
}