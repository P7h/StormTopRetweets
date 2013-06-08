package org.p7h.storm.retweets.bolts;

import java.util.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.Maps;
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
	private static final long serialVersionUID = 2161646804610027045L;
	/**
	 * For sorting the tweets on the # of retweets a particular tweet has got.
	 */
	private final static Comparator<Map.Entry<String, Status>> retweetCountComparator =
			new Comparator<Map.Entry<String, Status>>() {
				public int compare(Map.Entry<String, Status> status01, Map.Entry<String, Status> status02) {
					return (int) (status02.getValue().getRetweetCount() - status01.getValue().getRetweetCount());
				}
			};
	/**
	 * Interval between logging the output.
	 */
	private final long logIntervalInSeconds;
	/**
	 * Log only the retweets which crosses this threshold value.
	 */
	private final long rankMaxThreshold;

	private long lastLoggedTimestamp;
	private SortedMap<String, Status> retweetCountTracker;
	private long logFrequencyCounter;

	public RetweetCountBolt(final long logIntervalInSeconds, final long rankMaxThreshold) {
		this.logIntervalInSeconds = logIntervalInSeconds;
		this.rankMaxThreshold = rankMaxThreshold;
	}

	@Override
	public final void prepare(final Map map, final TopologyContext topologyContext,
	                          final OutputCollector collector) {
		lastLoggedTimestamp = System.currentTimeMillis();
		retweetCountTracker = Maps.newTreeMap();
		logFrequencyCounter = 0;
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
			logRetweetCount();
			lastLoggedTimestamp = timestampNow;
		}
	}

	private final void logRetweetCount() {
		Status status;
		final StringBuilder dumpRetweetInfoToLog = new StringBuilder();
		//Doing this circus so that we get sort the Map on the basis of # of retweets a tweets got.
		final List<Map.Entry<String, Status>> list = new ArrayList<>(retweetCountTracker.entrySet());
		Collections.sort(list, retweetCountComparator);
		int rankingTracker = 0;
		for (Map.Entry<String, Status> entry : list) {
			status = entry.getValue();
			dumpRetweetInfoToLog.append(status.getRetweetCount())
					.append(" ==> @")
					.append(status.getUser().getScreenName())
					.append(" | ")
					.append(status.getId())
					.append(" | ")
					.append(status.getText().replaceAll("\n", " "))
					.append("\n");
			rankingTracker++;
			if (rankingTracker == rankMaxThreshold) {
				break;
			}
		}

		++logFrequencyCounter;
		LOGGER.info("At {}, total # of retweeted tweets received in run#{}: {} ", new Date(),
				           logFrequencyCounter, retweetCountTracker.size());
		LOGGER.info("\n{}", dumpRetweetInfoToLog.toString());

		// Empty retweetCountTracker Maps for further iterations.
		retweetCountTracker.clear();
	}
}