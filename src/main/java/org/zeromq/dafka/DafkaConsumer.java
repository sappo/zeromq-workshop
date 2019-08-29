package org.zeromq.dafka;

import static org.zeromq.ZActor.SimpleActor;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.zeromq.ZActor;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

/**
 * <p>Skeleton actor for the DafkaConsumer</p>
 *
 * <p>The skeleton already has the DafkaBeacon implemented so discovering and connecting to peers is given.</p>
 */
public class DafkaConsumer extends SimpleActor {

  private static final Logger log = LogManager.getLogger(DafkaConsumer.class);

  private DafkaBeacon beacon;
  private ZActor beaconActor;

  public DafkaConsumer() {
    this.beacon = new DafkaBeacon();
  }

  @Override
  public List<Socket> createSockets(ZContext ctx, Object... args) {
    Properties properties = (Properties) args[0];

    this.beaconActor = new ZActor(ctx, this.beacon, null, args);
    this.beaconActor.recv(); // Wait for signal that beacon is connected to tower

    // HINT: Don't forget to return your sockets here otherwise they won't get passed as parameter into the start() method
    return Arrays.asList(beaconActor.pipe());
  }

  @Override
  public void start(Socket pipe, List<Socket> sockets, ZPoller poller) {
    String consumerAddress = UUID.randomUUID().toString();

    // TODO: Bind the consumers publisher socket to a random port and assign its value to this variable
    int publisherSocketPort = 666;

    beacon.start(beaconActor, consumerAddress, publisherSocketPort);
    boolean rc = poller.register(beaconActor.pipe(), ZPoller.IN);

    // HINT: This is the place where you want to subscribe to topics!

    // HINT: Don't forget to register your inbound sockets with the poller!

    // Signals the actor create about the successful startup by sending a zero byte.
    pipe.send(new byte[]{0});
    log.info("Consumer started...");
  }

  @Override
  public boolean finished(Socket pipe) {
    beacon.terminate(beaconActor);
    log.info("Consumer stopped!");
    return super.finished(pipe);
  }

  @Override
  public boolean stage(Socket socket, Socket pipe, ZPoller poller, int events) {
    // HINT: This is the place where you get notified about new messages on sockets registered with the poller.

    // HINT: It is useful to log the incoming message ;)

    if (socket.equals(beaconActor.pipe())) {
      String command = socket.recvStr();
      String address = socket.recvStr();

      if ("CONNECT".equals(command)) {
        log.info("Connecting to {}", address);
        // TODO: Connect your subscriber socket to the discovered peer's address
      } else if ("DISCONNECT".equals(command)) {
        log.info("Disconnecting from {}", address);
        // TODO: Disconnect your subscriber socket from the vanished peer's address
      } else {
        log.error("Transport: Unknown command {}", command);
        System.exit(1);
      }
    }

    return true;
  }

  @Override
  public boolean backstage(Socket pipe, ZPoller poller, int events) {
    // HINT: This is the place where you get notified about new messages from the creator of the actor.

    String command = pipe.recvStr();
    switch (command) {
      case "$TERM":
        return false;
      default:
        log.error("Invalid command {}", command);
    }
    return true;
  }

  /**
   * This methods subscribes a consumer to all partitions of a Dafka topic.
   *
   * @param topic Name of the topic
   */
  public void subscribe(String topic) {
    log.debug("Subscribe to topic {}", topic);

    // TODO: Implement me!
  }

  public void terminate(ZActor actor) {
    actor.send("$TERM");
  }

  public static void main(String[] args) throws InterruptedException, ParseException {
    Properties consumerProperties = new Properties();
    Options options = new Options();
    options.addOption("from_beginning", "Consume messages from beginning of partition");
    options.addOption("pub", true, "Tower publisher address");
    options.addOption("sub", true, "Tower subscriber address");
    options.addOption("verbose", "Enable verbose logging");
    options.addOption("help", "Displays this help");
    CommandLineParser parser = new DefaultParser();
    try {
      final CommandLine cmd = parser.parse(options, args);

      if (cmd.hasOption("help")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("dafka_console_consumer", options);
        return;
      }

      if (cmd.hasOption("verbose")) {
        Configurator.setRootLevel(Level.DEBUG);
      } else {
        Configurator.setRootLevel(Level.ERROR);
      }

      if (cmd.hasOption("from_beginning")) {
        consumerProperties.setProperty("consumer.offset.reset", "earliest");
      }
      if (cmd.hasOption("pub")) {
        consumerProperties.setProperty("beacon.pub_address", cmd.getOptionValue("pub"));
      }
      if (cmd.hasOption("sub")) {
        consumerProperties.setProperty("beacon.sub_address", cmd.getOptionValue("sub"));
      }
    } catch (UnrecognizedOptionException exception) {
      System.out.println(exception.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("dafka_console_consumer", options);
      return;
    }

    ZContext context = new ZContext();

    final DafkaConsumer dafkaConsumer = new DafkaConsumer();
    ZActor actor = new ZActor(context, dafkaConsumer, null, Arrays.asList(consumerProperties).toArray());

    // Wait until actor is ready
    Socket pipe = actor.pipe();
    byte[] signal = pipe.recv();
    assert signal[0] == 0;

    // Give time until connected to pubs and stores
    Thread.sleep(1000);
    dafkaConsumer.subscribe("HELLO");

    final Thread zmqThread = new Thread(() -> {
      while (!Thread.currentThread().isInterrupted()) {
        // TODO: Retrieve messages from subscribed topics and print them!

        // HINT: Call receive on the actor pipe
      }
    });

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      dafkaConsumer.terminate(actor);
      try {
        zmqThread.interrupt();
        zmqThread.join();
        context.close();
      } catch (InterruptedException e) {
      }
    }));

    zmqThread.start();
  }
}
