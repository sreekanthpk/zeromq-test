import java.util.Random;

import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

public class ZeroMQTest {
	 private static Random rand = new Random(System.nanoTime());
	
	 public static void main(String[] args) throws Exception {
	       
	       
	        new Thread(new serverTask()).start();

	        //  Run for 5 seconds then quit
	        Thread.sleep(5 * 1000);
	       
	    }
	 
	 private static class serverWorker implements Runnable {
	        private ZContext ctx;

	        public serverWorker(ZContext ctx) {
	            this.ctx = ctx;
	        }

	        public void run() {
	            Socket worker = ctx.createSocket(ZMQ.DEALER);
	            worker.connect("inproc://backend");

	            while (!Thread.currentThread().isInterrupted()) {
	                //  The DEALER socket gives us the address envelope and message
	                ZMsg msg = ZMsg.recvMsg(worker);
	                ZFrame address = msg.pop();
	                ZFrame content = msg.pop();
	                assert (content != null);
	                msg.destroy();

	                //  Send 0..4 replies back
	                int replies = rand.nextInt(5);
	                for (int reply = 0; reply < replies; reply++) {
	                    //  Sleep for some fraction of a second
	                    try {
	                        Thread.sleep(rand.nextInt(1000) + 1);
	                    } catch (InterruptedException e) {
	                    }
	                    address.send(worker, ZFrame.REUSE + ZFrame.MORE);
	                    content.send(worker, ZFrame.REUSE);
	                }
	                address.destroy();
	                content.destroy();
	            }
	            ctx.destroy();
	        }
	    }
	 
	 private static class serverTask implements Runnable {
	        public void run() {
	            ZContext ctx = new ZContext();

	            //  Frontend socket talks to clients over TCP
	            Socket frontend = ctx.createSocket(ZMQ.ROUTER);
	            frontend.bind("tcp://*:5570");

	            //  Backend socket talks to workers over inproc
	            Socket backend = ctx.createSocket(ZMQ.DEALER);
	            backend.bind("inproc://backend");

	            //  Launch pool of worker threads, precise number is not critical
	            for (int threadNbr = 0; threadNbr < 5; threadNbr++)
	                new Thread(new serverWorker(ctx)).start();

	            //  Connect backend to frontend via a proxy
	            ZMQ.proxy(frontend, backend, null);

	            ctx.destroy();
	        }
	    }
	 


	    private static class clientTask implements Runnable {

	        public void run() {
	            ZContext ctx = new ZContext();
	            Socket client = ctx.createSocket(ZMQ.DEALER);

	            //  Set random identity to make tracing easier
	            String identity = String.format("%04X-%04X", rand.nextInt(), rand.nextInt());
	            client.setIdentity(identity.getBytes());
	            client.connect("tcp://localhost:5570");

	            PollItem[] items = new PollItem[] { new PollItem(client, Poller.POLLIN) };

	            int requestNbr = 0;
	            while (!Thread.currentThread().isInterrupted()) {
	                //  Tick once per second, pulling in arriving messages
	                for (int centitick = 0; centitick < 100; centitick++) {
	                    ZMQ.poll(items, 10);
	                    if (items[0].isReadable()) {
	                        ZMsg msg = ZMsg.recvMsg(client);
	                        msg.getLast().print(identity);
	                        msg.destroy();
	                    }
	                }
	                client.send(String.format("request #%d", ++requestNbr), 0);
	            }
	            ctx.destroy();
	        }
	    }

}
