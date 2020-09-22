package org.apache.flink.streaming.examples.elias.rpc;

import akka.actor.ActorSystem;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceConfiguration;


/**
 * {@link RpcTest}
 *
 * @author <a href="mailto:siran0611@gmail.com">Elias.Yao</a>
 * @version ${project.version} - 2020/9/22
 */
public class RpcTest {
	private static final Time TIMEOUT = Time.seconds(10L);
	private static ActorSystem actorSystem = null;
	private static RpcService rpcService = null;

	public interface HelloGateway extends RpcGateway {
		String hello();
	}

	public interface HiGateway extends RpcGateway {
		String hi();
	}

	public static class HelloRpcEndpoint extends RpcEndpoint implements HelloGateway {

		protected HelloRpcEndpoint(RpcService rpcService) {
			super(rpcService);
		}

		@Override
		public String hello() {
			return "hello";
		}
	}

	public static class HiRpcEndpoint extends RpcEndpoint implements HiGateway {

		protected HiRpcEndpoint(RpcService rpcService) {
			super(rpcService);
		}

		@Override
		public String hi() {
			return "hi";
		}
	}

	public static void main(String[] args) throws Exception {
		// setup
		actorSystem = AkkaUtils.createDefaultActorSystem();
		rpcService = new AkkaRpcService(actorSystem, AkkaRpcServiceConfiguration.defaultConfiguration());


		HelloRpcEndpoint helloEndpoint = new HelloRpcEndpoint(rpcService);
		HiRpcEndpoint hiRpcEndpoint = new HiRpcEndpoint(rpcService);

		helloEndpoint.start();
		HelloGateway helloGateway = helloEndpoint.getSelfGateway(HelloGateway.class);
		String hello = helloGateway.hello();
		System.out.println(hello);

		hiRpcEndpoint.start();
		HiGateway hiGateway = rpcService.connect(hiRpcEndpoint.getAddress(), HiGateway.class).get();
		String hi = hiGateway.hi();
		System.out.println(hi);
	}
}
