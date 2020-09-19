package org.apache.flink.streaming.examples.elias.training.hourlytips;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.examples.elias.training.common.datatypes.TaxiFare;
import org.apache.flink.streaming.examples.elias.training.common.sources.TaxiFareGenerator;
import org.apache.flink.streaming.examples.elias.training.common.utils.ExerciseBase;
import org.apache.flink.util.Collector;

import static org.apache.flink.streaming.examples.elias.training.common.utils.ExerciseBase.fareSourceOrTest;
import static org.apache.flink.streaming.examples.elias.training.common.utils.ExerciseBase.printOrTest;

/**
 * {@link HourlyTipsExercise}
 *
 * @author <a href="mailto:siran0611@gmail.com">Elias.Yao</a>
 * @version ${project.version} - 2020/9/19
 */
public class HourlyTipsExercise {
	public static void main(String[] args) throws Exception {
		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareGenerator()));

		DataStream<Tuple3<Long, Long, Float>> hourlyMax =
			fares
				.keyBy(
					new KeySelector<TaxiFare, Long>() {
						@Override
						public Long getKey(TaxiFare value) throws Exception {
							return value.rideId;
						}
					})
				.window(TumblingEventTimeWindows.of(Time.hours(1)))
				.process(new Add())
				.windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
				.maxBy(2);

		printOrTest(hourlyMax);

		// execute the transformation pipeline
		env.execute("Hourly Tips (java)");
	}

	public static class Add
		extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {

		@Override
		public void process(
			Long key,
			Context context,
			Iterable<TaxiFare> fares,
			Collector<Tuple3<Long, Long, Float>> out)
			throws Exception {
			float total = 0F;
			for (TaxiFare fare : fares) {
				total += fare.tip;
			}
			out.collect(Tuple3.of(context.window().getEnd(), key, total));
		}
	}
}
