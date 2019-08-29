package org.zeromq.dafka;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.zeromq.ZActor;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.zeromq.ZActor.SimpleActor;

public class DafkaStore extends SimpleActor
{
    private static final Logger log = LogManager.getLogger(DafkaStore.class);

    private DafkaBeacon beacon;
    private ZActor beaconActor;

    @Override
    public List<Socket> createSockets(ZContext ctx, Object... args)
    {
        Properties properties = (Properties) args[0];
        String dbPath = properties.getProperty("store.db");

        this.beaconActor = new ZActor(ctx, this.beacon, null, args[1]);
        this.beaconActor.recv(); // Wait for signal that beacon is connected to tower

        // HINT: Don't forget to return your sockets here otherwise they won't get passed as parameter into the start() method
        return Arrays.asList();
    }

    @Override
    public void start(Socket pipe, List<Socket> sockets, ZPoller poller)
    {
        pipe.send(new byte[] { 0 });
        log.info("Store started...");
    }

    @Override
    public boolean finished(Socket pipe)
    {
        beacon.terminate(beaconActor);
        log.info("Store stopped!");
        return super.finished(pipe);
    }

    @Override
    public boolean stage(Socket socket, Socket pipe, ZPoller poller, int events)
    {
        // HINT: This is the place where you get notified about new messages on sockets registered with the poller.

        // HINT: It is useful to log the incoming message ;)

        if (socket.equals(beaconActor.pipe())) {
            String command = socket.recvStr();
            String address = socket.recvStr();

            if ("CONNECT".equals(command)) {
                log.info("Connecting to {}", address);
                // TODO: Connect your subscriber(s) socket to the discovered peer's address
            } else if ("DISCONNECT".equals(command)) {
                log.info("Disconnecting from {}", address);
                // TODO: Disconnect your subscriber(s) socket from the vanished peer's address
            } else {
                log.error("Transport: Unknown command {}", command);
                System.exit(1);
            }
        }

        return true;
    }

    @Override
    public boolean backstage(Socket pipe, ZPoller poller, int events)
    {
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

    public void terminate(ZActor actor)
    {
        actor.send("$TERM");
    }

    public static void main(String[] args) throws ParseException
    {
        Properties storeProperties = new Properties();
        org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
        options.addOption("pub", true, "Tower publisher address");
        options.addOption("sub", true, "Tower subscriber address");
        options.addOption("verbose", "Displays this help");
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
            }
            else {
                Configurator.setRootLevel(Level.ERROR);
            }

            if (cmd.hasOption("pub")) {
                storeProperties.setProperty("beacon.pub_address", cmd.getOptionValue("pub"));
            }
            if (cmd.hasOption("sub")) {
                storeProperties.setProperty("beacon.sub_address", cmd.getOptionValue("sub"));
            }
        }
        catch (UnrecognizedOptionException exception) {
            System.out.println(exception.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("dafka_stored", options);
            return;
        }

        storeProperties.setProperty("store.db", "./storedb");

        ZContext context = new ZContext();

        final DafkaStore dafkaStore = new DafkaStore();
        ZActor actor = new ZActor(context, dafkaStore, null, Arrays.asList(storeProperties).toArray());

        // Wait until actor is ready
        Socket pipe = actor.pipe();
        byte[] signal = pipe.recv();
        assert signal[0] == 0;

        final Thread zmqThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            dafkaStore.terminate(actor);
            try {
                zmqThread.interrupt();
                zmqThread.join();
                context.close();
            }
            catch (InterruptedException e) {
            }
        }));

        zmqThread.start();
    }
}
