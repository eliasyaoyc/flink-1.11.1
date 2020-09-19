package org.apache.flink.streaming.examples.elias.training.ridecleansing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.elias.training.common.datatypes.TaxiRide;
import org.apache.flink.streaming.examples.elias.training.common.sources.TaxiRideGenerator;
import org.apache.flink.streaming.examples.elias.training.common.utils.ExerciseBase;
import org.apache.flink.streaming.examples.elias.training.common.utils.GeoUtils;

/**
 * {@link RideCleansingExercise}
 *
 * @author <a href="mailto:siran0611@gmail.com">Elias.Yao</a>
 * @version ${project.version} - 2020/9/19
 */
public class RideCleansingExercise {
	public static void main(String[] args) throws Exception{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		DataStreamSource<TaxiRide> rides = env.addSource(ExerciseBase.rideSourceOrTest(new TaxiRideGenerator()));

		SingleOutputStreamOperator<TaxiRide> filteredRides = rides.filter(new NYCFilter());

		ExerciseBase.printOrTest(filteredRides);

		env.execute("Taxi Ride Cleansing");
	}

	public static class NYCFilter implements FilterFunction<TaxiRide>{

		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {
			return GeoUtils.isInNYC(taxiRide.startLon,taxiRide.startLat) &&
				GeoUtils.isInNYC(taxiRide.endLon,taxiRide.endLat);
		}
	}
}
