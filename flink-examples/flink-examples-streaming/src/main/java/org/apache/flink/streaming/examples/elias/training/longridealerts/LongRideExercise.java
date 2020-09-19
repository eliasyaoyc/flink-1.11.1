package org.apache.flink.streaming.examples.elias.training.longridealerts;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.examples.elias.training.common.datatypes.TaxiRide;
import org.apache.flink.streaming.examples.elias.training.common.sources.TaxiRideGenerator;
import org.apache.flink.streaming.examples.elias.training.common.utils.ExerciseBase;
import org.apache.flink.util.Collector;

/**
 * {@link LongRideExercise}
 *
 * @author <a href="mailto:siran0611@gmail.com">Elias.Yao</a>
 * @version ${project.version} - 2020/9/19
 */
public class LongRideExercise extends ExerciseBase{
	/**
	 * Main method.
	 *
	 * @throws Exception which occurs during job execution.
	 */
	public static void main(String[] args) throws Exception {

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()));

		DataStream<TaxiRide> longRides = rides
			.keyBy((TaxiRide ride) -> ride.rideId)
			.process(new MatchFunction());

		printOrTest(longRides);

		env.execute("Long Taxi Rides");
	}

	public static class MatchFunction extends KeyedProcessFunction<Long, TaxiRide, TaxiRide> {

		private ValueState<TaxiRide> rideState;

		@Override
		public void open(Configuration config) throws Exception {
			ValueStateDescriptor<TaxiRide> stateDescriptor = new ValueStateDescriptor<TaxiRide>("ride event",TaxiRide.class);
			rideState = getRuntimeContext().getState(stateDescriptor);
		}

		@Override
		public void processElement(TaxiRide ride, Context context, Collector<TaxiRide> out) throws Exception {
			TaxiRide previousRideEvent = rideState.value();

			if (previousRideEvent == null){
				rideState.update(ride);
				if (ride.isStart){
					context.timerService().registerEventTimeTimer(getTimerTime(ride));
				}
			}else {
				if (!ride.isStart){
					context.timerService().deleteEventTimeTimer(getTimerTime(previousRideEvent));
				}
				rideState.clear();
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext context, Collector<TaxiRide> out) throws Exception {
			out.collect(rideState.value());
			rideState.clear();
		}

		private long getTimerTime(TaxiRide ride){
			return ride.startTime.plusSeconds(120 * 60).toEpochMilli();
		}
	}
}
