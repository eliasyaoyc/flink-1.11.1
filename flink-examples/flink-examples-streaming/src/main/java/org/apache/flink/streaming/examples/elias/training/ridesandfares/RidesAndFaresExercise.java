package org.apache.flink.streaming.examples.elias.training.ridesandfares;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.examples.elias.training.common.datatypes.TaxiFare;
import org.apache.flink.streaming.examples.elias.training.common.datatypes.TaxiRide;
import org.apache.flink.streaming.examples.elias.training.common.sources.TaxiFareGenerator;
import org.apache.flink.streaming.examples.elias.training.common.sources.TaxiRideGenerator;
import org.apache.flink.streaming.examples.elias.training.common.utils.ExerciseBase;
import org.apache.flink.util.Collector;

import static org.apache.flink.streaming.examples.elias.training.common.utils.ExerciseBase.*;

/**
 * {@link RidesAndFaresExercise}
 *
 * @author <a href="mailto:siran0611@gmail.com">Elias.Yao</a>
 * @version ${project.version} - 2020/9/19
 */
public class RidesAndFaresExercise {

	public static void main(String[] args) throws Exception {
		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiRide> rides = env
			.addSource(rideSourceOrTest(new TaxiRideGenerator()))
			.filter((TaxiRide ride) -> ride.isStart)
			.keyBy((TaxiRide ride) -> ride.rideId);

		DataStream<TaxiFare> fares = env
			.addSource(fareSourceOrTest(new TaxiFareGenerator()))
			.keyBy((TaxiFare fare) -> fare.rideId);

		DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides
			.connect(fares)
			.flatMap(new EnrichmentFunction());

		printOrTest(enrichedRides);

		env.execute("Join Rides with Fares (java RichCoFlatMap)");
	}

	public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
		private ValueState<TaxiRide> taxiRide;
		private ValueState<TaxiFare> fareRide;

		@Override
		public void open(Configuration config) throws Exception {
			taxiRide = getRuntimeContext().getState(new ValueStateDescriptor<TaxiRide>("taxi_ride",TaxiRide.class));
			fareRide = getRuntimeContext().getState(new ValueStateDescriptor<TaxiFare>("taxi_fare",TaxiFare.class));
		}

		@Override
		public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiFare value = fareRide.value();
			if (value != null){
				fareRide.clear();
				out.collect(Tuple2.of(ride,value));
			}else {
				taxiRide.update(ride);
			}
		}

		@Override
		public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiRide value = taxiRide.value();
			if (value != null){
				taxiRide.clear();
				out.collect(Tuple2.of(value,fare));
			}else {
				fareRide.update(fare);
			}
		}
	}
}
