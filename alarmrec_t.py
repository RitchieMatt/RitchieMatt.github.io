#! /usr/bin/env python3

"""A simple processor for Asterisk Alarmreceiver events. It reads text files
written by the Asterisk alarm reciever, decodes them into a text string and then
publishes them to a MQTT broker.

Add the following command to alarmreceiver.conf
eventcmd = /home/pi/bin/alarmrec.py -v -e /tmp
"""

import argparse
import os
import logging
import logging.handlers
import re
import paho.mqtt.publish as mqtt

# Global Logger
_LOG = logging.getLogger('alarmreceiver')

_LOG = logging.getLogger()
_HANDLER = logging.StreamHandler()
_FORMATTER = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
_HANDLER.setFormatter(_FORMATTER)
_LOG.addHandler(_HANDLER)

class AlarmEventFile():
    """Reads a file written by Asterisk alarmreceiver which can contain one or more
       events
    """
    unknown_event = ('unknown', 'unknown', 'unknown')

    event_dict = {
        100 : ("alert", "Medical Alert", "zone"),
        101 : ("alert", "Personal Emergency", "zone"),
        102 : ("alert", "Failure to Report In", "zone"),
        110 : ("alert", "Fire Alarm", "zone"),
        111 : ("alert", "Smoke Alarm", "zone"),
        112 : ("alert", "Combustion Detected Alarm", "zone"),
        113 : ("alert", "Water Flood Alarm", "zone"),
        114 : ("alert", "Excessive Heat Alarm", "zone"),
        115 : ("alert", "Fire Alarm Pulled", "zone"),
        116 : ("alert", "Duct Alarm", "zone"),
        117 : ("alert", "Flame Detected", "zone"),
        118 : ("alert", "Near Alarm", "zone"),
        120 : ("alert", "Panic Alarm", "zone"),
        121 : ("alert", "Duress Alarm", "user"),
        122 : ("alert", "Alarm, 24-hour Silent", "zone"),
        123 : ("alert", "Alarm, 24-hour Audible", "zone"),
        124 : ("alert", "Duress - Access granted", "zone"),
        125 : ("alert", "Duress - Egress granted", "zone"),
        130 : ("alert", "Alarm, 24-hour Audible", "zone"),
        131 : ("alert", "Alarm, Perimeter", "zone"),
        132 : ("alert", "Alarm, Interior", "zone"),
        133 : ("alert", "24 Hour (Safe)", "zone"),
        134 : ("alert", "Alarm, Entry/Exit", "zone"),
        135 : ("alert", "Alarm, Day/Night", "zone"),
        136 : ("alert", "Alarm, Outdoor", "zone"),
        137 : ("alert", "Alarm, Tamper", "zone"),
        138 : ("alert", "Near Alarm", "zone"),
        139 : ("alert", "Intrusion Verifier", "zone"),
        140 : ("alert", "Alarm, General Alarm", "zone"),
        141 : ("alert", "Alarm, Polling Loop Open", "zone"),
        142 : ("alert", "Alarm, Polling Loop Short", "zone"),
        143 : ("alert", "Alarm, Expansion Module", "zone"),
        144 : ("alert", "Alarm, Sensor Tamper", "zone"),
        145 : ("alert", "Alarm, Expansion Module Tamper", "zone"),
        146 : ("alert", "Silent Burglary", "zone"),
        147 : ("alert", "Sensor Supervision failure", "zone"),
        150 : ("alert", "Alarm, 24-Hour Auxiliary", "zone"),
        151 : ("alert", "Alarm, Gas detected", "zone"),
        152 : ("alert", "Alarm, Refrigeration", "zone"),
        153 : ("alert", "Alarm, Loss of heat", "zone"),
        154 : ("alert", "Alarm, Water leakage", "zone"),
        155 : ("alert", "Alarm, foil break", "zone"),
        156 : ("alert", "Day trouble", "zone"),
        157 : ("alert", "Low bottled gas level", "zone"),
        158 : ("alert", "Alarm, High temperature", "zone"),
        159 : ("alert", "Alarm, Low temperature", "zone"),
        161 : ("alert", "Alarm, Loss of air flow", "zone"),
        162 : ("alert", "Alarm, Carbon Monoxide Detected", "zone"),
        163 : ("alert", "Alarm, Tank Level", "zone"),
        300 : ("alert", "System Trouble", "zone"),
        301 : ("alert", "AC Power", "zone"),
        302 : ("alert", "Low System Battery/Battery Test Fail", "zone"),
        303 : ("alert", "RAM Checksum Bad", "zone"),
        304 : ("alert", "ROM Checksum Bad", "zone"),
        305 : ("alert", "System Reset", "zone"),
        306 : ("alert", "Panel programming changed", "zone"),
        307 : ("alert", "Self-test failure", "zone"),
        308 : ("alert", "System shutdown", "zone"),
        309 : ("alert", "Battery test failure", "zone"),
        310 : ("alert", "Ground fault", "zone"),
        311 : ("alert", "Battery Missing/Dead", "zone"),
        312 : ("alert", "Power Supply Overcurrent", "zone"),
        313 : ("alert", "Engineer Reset", "user"),
        321 : ("alert", "Bell/Siren Trouble", "zone"),
        333 : ("alert", "Trouble or Tamper Expansion Module", "zone"),
        341 : ("alert", "Trouble, ECP Cover Tamper", "zone"),
        344 : ("alert", "RF Receiver Jam", "zone"),
        351 : ("alert", "Telco Line Fault", "zone"),
        353 : ("alert", "Long Range Radio Trouble", "zone"),
        373 : ("alert", "Fire Loop Trouble", "zone"),
        374 : ("alert", "Exit Error Alarm", "zone"),
        380 : ("alert", "Global Trouble, Trouble Day/Night", "zone"),
        381 : ("alert", "RF Supervision Trouble", "zone"),
        382 : ("alert", "Supervision Auxillary Wire Zone", "zone"),
        383 : ("alert", "RF Sensor Tamper", "zone"),
        384 : ("alert", "RF Sensor Low Battery", "zone"),
        393 : ("alert", "Clean Me", "zone"),
        401 : ("alert", "AWAY/MAX", "user"),
        403 : ("alert", "Scheduled Arming", "user"),
        406 : ("alert", "Cancel by User", "user"),
        407 : ("alert", "Remote Arm/Disarm (Downloading)", "user"),
        408 : ("alert", "Quick AWAY/MAX", "user"),
        409 : ("alert", "AWAY/MAX Keyswitch", "user"),
        411 : ("alert", "Callback Requested", "user"),
        412 : ("alert", "Success-Download/Access", "user"),
        413 : ("alert", "Unsuccessful Access", "user"),
        414 : ("alert", "System Shutdown", "user"),
        415 : ("alert", "Dialer Shutdown", "user"),
        416 : ("alert", "Successful Upload", "user"),
        421 : ("alert", "Access Denied", "user"),
        422 : ("alert", "Access Granted", "user"),
        423 : ("alert", "PANIC Forced Access", "zone"),
        424 : ("alert", "Egress Denied", "user"),
        425 : ("alert", "Egress Granted", "user"),
        426 : ("alert", "Access Door Propped Open", "zone"),
        427 : ("alert", "Access Point DSM Trouble", "zone"),
        428 : ("alert", "Access Point RTE Trouble", "zone"),
        429 : ("alert", "Access Program Mode Entry", "user"),
        430 : ("alert", "Access Program Mode Exit", "user"),
        431 : ("alert", "Access Threat Level Change", "user"),
        432 : ("alert", "Access Relay/Triger Failure", "zone"),
        433 : ("alert", "Access RTE Shunt", "zone"),
        434 : ("alert", "Access DSM Shunt", "zone"),
        441 : ("alert", "STAY/INSTANT", "user"),
        442 : ("alert", "STAY/INSTANT Keyswitch", "user"),
        570 : ("alert", "Zone Bypass", "zone"),
        574 : ("alert", "Group Bypass", "user"),
        601 : ("alert", "Operator Initiated Dialer Test", "user"),
        602 : ("alert", "Periodic Test", "zone"),
        606 : ("alert", "AAV to follow", "zone"),
        607 : ("alert", "Walk Test", "user"),
        623 : ("alert", "Event Log 80% Full", "zone"),
        625 : ("alert", "Real-Time Clock Changed", "user"),
        627 : ("alert", "Program Mode Entry", "zone"),
        628 : ("alert", "Program Mode Exit", "zone"),
        629 : ("alert", "1-1/3 Day No Event", "zone"),
        642 : ("alert", "Latch Key", "zone")}
        # 652, 653 -- all user events
        # 461, 465 -- all zone

    """Class representing AlarmEvents"""
    def __init__(self, alarmreciever_file=None):
        """Construct alarm event class from file"""
        self.file = alarmreciever_file
        self.alert_number = None
        self.time = None
        self.alarm_events = []

        raw_events = self.parse()
        for raw_event in raw_events:
            self.alarm_events.append(self.classify(raw_event))

    def __iter__(self):
        for event in self.alarm_events:
            yield event

    def parse(self):
        """Read and parse the lines from the event file and return a list
        of raw 16 digit strings
        """
        # Attempt to read the alarm event file
        try:
            wholefile = open(self.file, "r").read()
        except IOError:
            _LOG.error("Failed to read event file: %s", self.file)
            return []

        timestamp_re = re.compile('TIMESTAMP=(?P<timestamp>.*)')
        timestamp_match = timestamp_re.search(wholefile)

        if timestamp_match is None:
            _LOG.warning("Warning event had no timestamp")

        else:
            self.time = timestamp_match.group()
            _LOG.debug("Event at time %s", self.time)

        # Throw away everything before the event header and then find
        # the 16 digit event strings
        event_header = '[events]'
        if event_header in wholefile:
            [dummy_metadata, events] = wholefile.split(event_header)

        event_re = re.compile(r'([ABCD\d]{16})')

        return event_re.findall(events)

    def classify(self, eventstring):
        """Parse a raw 16 digit ContactID string and return an AlarmEvent
        """

        # acctnum[0:4]
        # mt[4:6] message type (18, 98)
        # q[6:7]  1 = new open, 3 = new close, 6 = status report
        event = eventstring[7:10]    # event code
        # p[10:12] partion
        zonenum = eventstring[12:15] #  zone
        # cksum [15] checksum

        try:
            event_index = int(event)
            (event_type, description, datatype) = self.event_dict[event_index]
        except KeyError:
            _LOG.info("Unknown event code %s in %s", event, eventstring)
            (event_type, description, datatype) = self.unknown_event

        _LOG.debug("Event: %s (%s) %s=%s", event_type, description, datatype, zonenum)

        return (event_type, description, datatype, zonenum)

def event_directory(string):
    """Helper function for argparser"""
    if not os.path.isdir(string):
        raise argparse.ArgumentTypeError("%s is not a directory", string)
    return string

def main():
    """Main function, parse args and read alarm files, send resulting events to mqtt broker"""
    parser = argparse.ArgumentParser(description="An Asterisk Alarmreciever to MQTT inteface",
                                     epilog="Version 0.1 by Dave Sargeant")

    parser.add_argument("-e", "--eventdir", help="The directory containing event files",
                        type=event_directory)
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="increase output verbosity")
    parser.add_argument("-m", "--mqtt-broker", dest='mqtt_broker', default='localhost',
                        help="Relay events to mqtt server")

    args = parser.parse_args()

    syslog_handler = logging.handlers.SysLogHandler(address='/dev/log')
    _LOG.addHandler(syslog_handler)

    if args.verbose is True:
        _LOG.setLevel(logging.DEBUG)

    _LOG.info("Event dir %s", args.eventdir)
    for root, dummy_dirs, files in os.walk(args.eventdir):
        for alarmrec_file in files:
            if alarmrec_file.startswith("event-") and not alarmrec_file.endswith(".handled"):
                file_and_path = os.path.join(root, alarmrec_file)
                _LOG.info("Handling event file %s", file_and_path)
                eventfile = AlarmEventFile(file_and_path)
                try:
                    os.rename(file_and_path, "{}.handled".format(file_and_path))
                except OSError as os_err:
                    _LOG.error("Unable to rename file :%s", str(os_err))
                    continue

                msgs = []
                for (event_type, description, datatype, zonenum) in eventfile:
                    payload = "{description} {datatype}={data}".format(description=description,
                                                                       datatype=datatype,
                                                                       data=zonenum)
                    topic = "whouse/alexor/{}".format(event_type)

                    msg = {'topic':topic,
                           'payload':payload}
                    _LOG.debug("Adding (topic=%s) : %s to %s", topic, payload, args.mqtt_broker)
                    msgs.append(msg)

                if len(msgs):
                    _LOG.debug("Sending %d message to broker %s", len(msgs), args.mqtt_broker)
                    mqtt.multiple(msgs, hostname=args.mqtt_broker)


if __name__ == "__main__":
    main()
