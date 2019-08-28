package org.zeromq.dafka;

import static org.zeromq.ZActor.SimpleActor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.zeromq.ZActor;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;
import org.zeromq.ZTimer;

public class DafkaProducer extends SimpleActor {

  private static final Logger log = LogManager.getLogger(DafkaProducer.class);

  private Thread timerThread;
  private boolean timerThreadRunning;
  private ZTimer ztimer;
  private ZTimer.Timer headTimer;

  private DafkaBeacon beacon;
  private ZActor beaconActor;

  public DafkaProducer() {
    // In order for a ZTimer to run properly its needs its own thread!
    this.ztimer = new ZTimer();
    timerThread = new Thread(() -> {
      while (timerThreadRunning) {
        ztimer.sleepAndExecute();
      }
    });
    timerThreadRunning = true;
    timerThread.start();

    /* Register your head timer job like this:
      headTimer = ztimer.add(interval, args -> {
        // Timer actions
      }, handlerFunctionArguments);
    */

    this.beacon = new DafkaBeacon();
  }

  @Override
  public List<Socket> createSockets(ZContext ctx, Object... args) {
    String topic = (String) args[0];
    Properties properties = (Properties) args[1];

    this.beaconActor = new ZActor(ctx, this.beacon, null, args[1]);
    this.beaconActor.recv(); // Wait for signal that beacon is connected to tower

    // HINT: Don't forget to return your sockets here otherwise they won't get passed as parameter into the start() method
    return Arrays.asList(beaconActor.pipe());
  }

  @Override
  public void start(Socket pipe, List<Socket> sockets, ZPoller poller) {
    String producerAddress = UUID.randomUUID().toString();

    // TODO: Bind the consumers publisher socket to a random port and assign its value to this variable
    int publisherSocketPort = 666;

    beacon.start(beaconActor, producerAddress, publisherSocketPort);

    // HINT: This is the place where you want to subscribe to topics!

    // HINT: Don't forget to register your inbound sockets with the poller!

    // Signals the actor create about the successful startup by sending a zero byte.
    pipe.send(new byte[]{0});
    log.info("Producer started...");
  }

  @Override
  public boolean finished(Socket pipe) {
    timerThreadRunning = false;
    synchronized (timerThread) {
      try {
        timerThread.wait();
      } catch (InterruptedException exception) {
        log.error("Failed to stop timer thread", exception);
      }
    }
    beacon.terminate(beaconActor);
    log.info("Producer stopped!");
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
        assert (false);
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
        if (headTimer != null) {
          ztimer.cancel(headTimer);
          headTimer = null;
        }
        return false;
      default:
        log.error("Invalid command {}", command);
    }
    return true;
  }

  public void terminate(ZActor actor) {
    actor.send("$TERM");
  }

  public static void main(String[] args) throws ParseException {
    Properties consumerProperties = new Properties();
    Options options = new Options();
    options.addRequiredOption("topic", "topic", true, "Topic name the publisher publishes to");
    options.addOption("pub", true, "Tower publisher address");
    options.addOption("sub", true, "Tower subscriber address");
    options.addOption("verbose", "Enable verbose logging");
    options.addOption("help", "Displays this help");
    CommandLineParser parser = new DefaultParser();
    String topic;
    try {
      final CommandLine cmd = parser.parse(options, args);

      topic = cmd.getOptionValue("topic");

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

      if (cmd.hasOption("pub")) {
        consumerProperties.setProperty("beacon.pub_address", cmd.getOptionValue("pub"));
      }
      if (cmd.hasOption("sub")) {
        consumerProperties.setProperty("beacon.sub_address", cmd.getOptionValue("sub"));
      }
    } catch (UnrecognizedOptionException exception) {
      System.out.println(exception.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("dafka_console_producer", options);
      return;
    }

    ZContext context = new ZContext();

    final DafkaProducer dafkaProducer = new DafkaProducer();
    ZActor actor = new ZActor(context, dafkaProducer, null, Arrays.asList(topic, consumerProperties).toArray());

    // Wait until actor is ready
    Socket pipe = actor.pipe();
    byte[] signal = pipe.recv();
    assert signal[0] == 0;

    final Thread zmqThread = new Thread(() -> {
      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
      while (!Thread.currentThread().isInterrupted()) {
        try {
          if (reader.ready()) {
            String line = reader.readLine();
            if (StringUtils.isNoneBlank(line)) {
              // TODO: Publish "line" as event to this publisher's topic partition
            }
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      dafkaProducer.terminate(actor);
    });

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
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
