#!/usr/bin/env python

import os
import sys
from script_utils import SCRIPT_HOME, VERSION
sys.path.insert(1, os.path.join(os.path.dirname(__file__), f"{SCRIPT_HOME}/ext"))

import dbus
from dbus.mainloop.glib import DBusGMainLoop
from gi.repository import GLib
import logging
from vedbus import VeDbusService
from dbusmonitor import DbusMonitor
from collections import namedtuple
import time

DEVICE_INSTANCE_ID = 1025
PRODUCT_ID = 0
PRODUCT_NAME = "Battery Proxy"
FIRMWARE_VERSION = 0
HARDWARE_VERSION = 0
CONNECTED = 1

FULL_VOLTAGE = 12.8
EMPTY_VOLTAGE = 11.8

HIGH_VOLTAGE_ALARM = 14.8
LOW_VOLTAGE_ALARM = 12.2

ALARM_OK = 0
ALARM_WARNING = 1
ALARM_ALARM = 2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("battery")


class SystemBus(dbus.bus.BusConnection):
    def __new__(cls):
        return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SYSTEM)


class SessionBus(dbus.bus.BusConnection):
    def __new__(cls):
        return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SESSION)


def dbusConnection():
    return SessionBus() if 'DBUS_SESSION_BUS_ADDRESS' in os.environ else SystemBus()


Service = namedtuple('Service', ['name', 'type'])
PowerSample = namedtuple('PowerSample', ['power', 'timestamp'])


def _safe_min(newValue, currentValue):
    return min(newValue, currentValue) if currentValue else newValue


def _safe_max(newValue, currentValue):
    return max(newValue, currentValue) if currentValue else newValue


def toKWh(joules):
    return joules/3600/1000


def soc_from_voltage(voltage):
    # very approximate!!!
    return 100 * (voltage - EMPTY_VOLTAGE)/(FULL_VOLTAGE - EMPTY_VOLTAGE)


class BatteryService:
    def __init__(self, conn):
        self.service = VeDbusService('com.victronenergy.battery.proxy', conn)
        self.service.add_mandatory_paths(__file__, VERSION, 'dbus', DEVICE_INSTANCE_ID,
                                     PRODUCT_ID, PRODUCT_NAME, FIRMWARE_VERSION, HARDWARE_VERSION, CONNECTED)
        self.service.add_path("/Dc/0/Voltage", 0, gettextcallback=lambda path,value: "{:.2f}V".format(value))
        self.service.add_path("/Dc/0/Current", 0, gettextcallback=lambda path,value: "{:.3f}A".format(value))
        self.service.add_path("/Dc/0/Power", 0, gettextcallback=lambda path,value: "{:.2f}W".format(value))
        self.service.add_path("/Soc", None, gettextcallback=lambda path,value: "{:.0f}%".format(value))
        self.service.add_path("/History/MinimumVoltage", None, gettextcallback=lambda path,value: "{:.2f}V".format(value))
        self.service.add_path("/History/MaximumVoltage", None, gettextcallback=lambda path,value: "{:.2f}V".format(value))
        self.service.add_path("/History/ChargedEnergy", 0, gettextcallback=lambda path,value: "{:.6f}kWh".format(value))
        self.service.add_path("/History/DischargedEnergy", 0, gettextcallback=lambda path,value: "{:.6f}kWh".format(value))
        self.service.add_path("/Alarms/LowVoltage", ALARM_OK)
        self.service.add_path("/Alarms/HighVoltage", ALARM_OK)
        options = None  # currently not used afaik
        self.monitor = DbusMonitor({
            'com.victronenergy.solarcharger': {
                '/Dc/0/Current': options,
                '/Dc/0/Voltage': options
            },
            'com.victronenergy.dcload': {
                '/Dc/0/Current': options,
                '/Dc/0/Voltage': options
            },
            'com.victronenergy.dcsource': {
                '/Dc/0/Current': options,
                '/Dc/0/Voltage': options
            }
        })
        self.lastInPower = None
        self.lastOutPower = None

    def _get_value(self, serviceName, path, defaultValue=None):
        return self.monitor.get_value(serviceName, path, defaultValue)

    def update(self):
        bestLoadVoltage = None
        bestSourceVoltage = None
        inCurrent = 0
        outCurrent = 0

        services = []
        for serviceType in ['solarcharger', 'dcload', 'dcsource']:
            for serviceName in self.monitor.get_service_list('com.victronenergy.' + serviceType):
                services.append(Service(serviceName, serviceType))

        for service in services:
            serviceName = service.name
            current = self._get_value(serviceName, "/Dc/0/Current", 0)
            voltage = self._get_value(serviceName, "/Dc/0/Voltage")
            if service.type == 'dcload':
                outCurrent += current
                # highest should be most accurate as closest to battery (upstream cable losses)
                if voltage:
                    bestLoadVoltage = _safe_max(voltage, bestLoadVoltage)
            else:
                inCurrent += current
                # lowest should be most accurate as closest to battery (downstream cable losses)
                if voltage:
                    bestSourceVoltage = _safe_min(voltage, bestSourceVoltage)

        totalCurrent = inCurrent - outCurrent

        self.service["/Dc/0/Current"] = round(totalCurrent, 3)
        batteryVoltage = None
        if bestLoadVoltage and bestSourceVoltage:
            batteryVoltage = (bestLoadVoltage + bestSourceVoltage)/2
        elif bestLoadVoltage:
            batteryVoltage = bestLoadVoltage
        elif bestSourceVoltage:
            batteryVoltage = bestSourceVoltage
        if batteryVoltage:
            self.service["/Dc/0/Voltage"] = round(batteryVoltage, 3)
            self.service["/Dc/0/Power"] = round(batteryVoltage * totalCurrent, 3)
            self.service["/Soc"] = soc_from_voltage(batteryVoltage)
            self.service["/History/MinimumVoltage"] = _safe_min(batteryVoltage, self.service["/History/MinimumVoltage"])
            self.service["/History/MaximumVoltage"] = _safe_max(batteryVoltage, self.service["/History/MaximumVoltage"])

            now = time.perf_counter()
            inPower = batteryVoltage * inCurrent
            if self.lastInPower is not None:
                # trapezium integration
                self.service["/History/ChargedEnergy"] += round(toKWh((self.lastInPower.power + inPower)/2 * (now - self.lastInPower.timestamp)), 7)
            self.lastInPower = PowerSample(inPower, now)

            outPower = batteryVoltage * outCurrent
            if self.lastOutPower is not None:
                # trapezium integration
                self.service["/History/DischargedEnergy"] += round(toKWh((self.lastOutPower.power + outPower)/2 * (now - self.lastOutPower.timestamp)), 7)
            self.lastOutPower = PowerSample(outPower, now)

            if batteryVoltage <= LOW_VOLTAGE_ALARM:
                self.service["/Alarms/LowVoltage"] = ALARM_ALARM
            if batteryVoltage >= HIGH_VOLTAGE_ALARM:
                self.service["/Alarms/HighVoltage"] = ALARM_ALARM
        return True

    def __str__(self):
        return PRODUCT_NAME


def main():
    DBusGMainLoop(set_as_default=True)
    battery = BatteryService(dbusConnection())
    GLib.timeout_add(1000, battery.update)
    logger.info("Registered Battery Proxy")
    mainloop = GLib.MainLoop()
    mainloop.run()


if __name__ == "__main__":
    main()
