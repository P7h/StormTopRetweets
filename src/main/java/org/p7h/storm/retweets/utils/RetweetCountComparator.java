package org.p7h.storm.retweets.utils;

import java.util.Comparator;
import java.util.Map;

import twitter4j.Status;

/**
 * Sorts tweets on the # of retweets a particular tweet has got.
 * User: 078831
 * Date: 6/14/13
 * Time: 11:17 AM
 * To change this template use File | Settings | File Templates.
 */
public final class RetweetCountComparator implements Comparator<Map.Entry<String, Status>> {
	@Override
	public final int compare(final Map.Entry<String, Status> status01,
	                         final Map.Entry<String, Status> status02) {
		final long retweetCount01 = status01.getValue().getRetweetCount();
		final long retweetCount02 = status02.getValue().getRetweetCount();
		int compareResult;
		if (retweetCount02 > retweetCount01) {
			compareResult = 1;
		} else if (retweetCount02 < retweetCount01) {
			compareResult = -1;
		} else {
			compareResult = 0;
		}
		return compareResult;
	}
}