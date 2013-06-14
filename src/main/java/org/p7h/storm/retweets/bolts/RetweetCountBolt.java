package org.p7h.storm.retweets.bolts;

import java.util.*;
import java.util.concurrent.TimeUnit;

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
import org.p7h.storm.retweets.utils.RetweetCountComparator;
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
	private static final long serialVersionUID = -3821987785264060963L;

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
		stopwatch = new Stopwatch();
		stopwatch.start();
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

	private final void logRetweetCount() {
		//Doing this circus so that we can sort the Map on the basis of # of retweets a tweet got.
		List<Map.Entry<String, Status>> list = new ArrayList<>(retweetCountTracker.entrySet());
		Collections.sort(list, new RetweetCountComparator());

		if(rankMaxThreshold < list.size()) {
			list = list.subList(0, rankMaxThreshold);
		}

		final List<String> newList = Lists.transform(list, new Function<Map.Entry<String, Status>, String>() {
			@Override
			public String apply(final java.util.Map.Entry<String, Status> input) {
				final Status status = input.getValue();
				final StringBuilder retweetAnalysis = new StringBuilder();
				retweetAnalysis.append(status.getRetweetCount())
						.append(" ==> @")
						.append(status.getUser().getScreenName())
						.append(" | ")
						.append(status.getId())
						.append(" | ")
						.append(status.getText().replaceAll("\n", " "));
				return retweetAnalysis.toString();
			}
		});
		final String retweetInfo = Joiner.on('\n').join(newList);

		runCounter++;
		LOGGER.info("\n\nAt {}, total # of retweeted tweets received in run#{}: {}", new Date(),
				           runCounter, retweetCountTracker.size());
		LOGGER.info("\n{}", retweetInfo);

		// Empty retweetCountTracker Maps for further iterations.
		retweetCountTracker.clear();
	}
}