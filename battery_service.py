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
from pathlib import Path
import json

FULL_VOLTAGE = 12.8
EMPTY_VOLTAGE = 11.8

DEPTH_OF_DISCHARGE = 50

STANDARD_TEMPERATURE = 25
TEMPERATURE_COMPENSATION = -16/1000

HIGH_VOLTAGE_ALARM = 14.8
LOW_VOLTAGE_ALARM = 12.2

VOLTAGE_DEADBAND = 1.0

MAX_DATA_HISTORY = 9

DEVICE_INSTANCE_ID = 1025
PRODUCT_ID = 0
PRODUCT_NAME = "Battery Proxy"
FIRMWARE_VERSION = 0
HARDWARE_VERSION = 0
CONNECTED = 1

BATTERY_TEMPERATURE_SENSOR = 0

FLOAT_STATE = 5

FOREVER = 864000

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
DataSample = namedtuple('DataSample', ['current', 'voltage', 'timestamp', 'temperature'])


def _safe_min(newValue, currentValue):
    return min(newValue, currentValue) if currentValue else newValue


def _safe_max(newValue, currentValue):
    return max(newValue, currentValue) if currentValue else newValue


def toKWh(joules):
    return joules/3600/1000


def toAh(joules, voltage):
    return joules/voltage/3600


VOLTAGE_TEXT = lambda path,value: "{:.2f}V".format(value)
CURRENT_TEXT = lambda path,value: "{:.3f}A".format(value)
POWER_TEXT = lambda path,value: "{:.2f}W".format(value)
ENERGY_TEXT = lambda path,value: "{:.6f}kWh".format(value)
AH_TEXT = lambda path,value: "{:.3f}Ah".format(value)
SOC_TEXT = lambda path,value: "{:.0f}%".format(value)


def compensated_voltage(voltage, temperature):
    return voltage - (temperature - STANDARD_TEMPERATURE) * TEMPERATURE_COMPENSATION


def soc_from_voltage(voltage):
    # very approximate!!!
    return min(max(100 * (voltage - EMPTY_VOLTAGE)/(FULL_VOLTAGE - EMPTY_VOLTAGE), 0), 100)


class BatteryService:
    def __init__(self, conn, config):
        self.config = config
        self.service = VeDbusService('com.victronenergy.battery.proxy', conn)
        self.service.add_mandatory_paths(__file__, VERSION, 'dbus', DEVICE_INSTANCE_ID,
                                     PRODUCT_ID, PRODUCT_NAME, FIRMWARE_VERSION, HARDWARE_VERSION, CONNECTED)
        self.service.add_path("/Dc/0/Voltage", 0, gettextcallback=VOLTAGE_TEXT)
        self.service.add_path("/Dc/0/Current", 0, gettextcallback=CURRENT_TEXT)
        self.service.add_path("/Dc/0/Power", 0, gettextcallback=POWER_TEXT)
        self.service.add_path("/Soc", None, gettextcallback=SOC_TEXT)
        self.service.add_path("/TimeToGo", FOREVER)
        self.service.add_path("/History/MinimumVoltage", None, gettextcallback=VOLTAGE_TEXT)
        self.service.add_path("/History/MaximumVoltage", None, gettextcallback=VOLTAGE_TEXT)
        self.service.add_path("/History/ChargedEnergy", 0, gettextcallback=ENERGY_TEXT)
        self.service.add_path("/History/DischargedEnergy", 0, gettextcallback=ENERGY_TEXT)
        self.service.add_path("/History/TotalAhDrawn", 0, gettextcallback=AH_TEXT)
        self.service.add_path("/History/DeepestDischarge", None, gettextcallback=SOC_TEXT)
        self.service.add_path("/History/FullDischarges", 0)
        self.service.add_path("/Alarms/LowVoltage", ALARM_OK)
        self.service.add_path("/Alarms/HighVoltage", ALARM_OK)
        self.service.add_path("/Alarms/LowSoc", ALARM_OK)
        self.service.add_path("/RemainingAmphours", self.config['capacity'], gettextcallback=AH_TEXT)
        options = None  # currently not used afaik
        self.monitor = DbusMonitor({
            'com.victronenergy.solarcharger': {
                '/Dc/0/Current': options,
                '/Dc/0/Voltage': options,
                '/State': options
            },
            'com.victronenergy.dcload': {
                '/Dc/0/Current': options,
                '/Dc/0/Voltage': options
            },
            'com.victronenergy.dcsource': {
                '/Dc/0/Current': options,
                '/Dc/0/Voltage': options
            },
            'com.victronenergy.temperature': {
                '/Temperature': options,
                '/TemperatureType': options
            }
        })
        self.lastPower = None
        self.dataHistory = []

    def _get_value(self, serviceName, path, defaultValue=None):
        return self.monitor.get_value(serviceName, path, defaultValue)

    def update(self):
        bestLoadVoltage = None
        bestSourceVoltage = None
        totalCurrent = 0
        chargingState = None

        services = []
        for serviceType in ['solarcharger', 'dcload', 'dcsource']:
            for serviceName in self.monitor.get_service_list('com.victronenergy.' + serviceType):
                services.append(Service(serviceName, serviceType))

        for service in services:
            serviceName = service.name
            current = self._get_value(serviceName, "/Dc/0/Current", 0)
            voltage = self._get_value(serviceName, "/Dc/0/Voltage", 0)
            if service.type == 'dcload':
                current = -current
                # highest should be most accurate as closest to battery (upstream cable losses)
                if voltage > VOLTAGE_DEADBAND:
                    bestLoadVoltage = _safe_max(voltage, bestLoadVoltage)
            else:
                # lowest should be most accurate as closest to battery (downstream cable losses)
                if voltage > VOLTAGE_DEADBAND:
                    bestSourceVoltage = _safe_min(voltage, bestSourceVoltage)
            totalCurrent += current

            if service.type == 'solarcharger':
                chargingState = self._get_value(serviceName, "/State")

        temperature = STANDARD_TEMPERATURE
        for serviceName in self.monitor.get_service_list('com.victronenergy.temperature'):
            if self._get_value(serviceName, "/TemperatureType") == BATTERY_TEMPERATURE_SENSOR:
                temperature = self._get_value(serviceName, "/Temperature", STANDARD_TEMPERATURE)
                break

        self.service["/Dc/0/Current"] = totalCurrent
        batteryVoltage = None
        if bestLoadVoltage and bestSourceVoltage:
            batteryVoltage = (bestLoadVoltage + bestSourceVoltage)/2
        elif bestLoadVoltage:
            batteryVoltage = bestLoadVoltage
        elif bestSourceVoltage:
            batteryVoltage = bestSourceVoltage
        if batteryVoltage:
            self.service["/Dc/0/Voltage"] = round(batteryVoltage, 3)

            now = time.perf_counter()
            power = batteryVoltage * totalCurrent
            self.service["/Dc/0/Power"] = round(power, 3)
            remainingAh = self.service["/RemainingAmphours"]
            if self.lastPower is not None:
                # trapezium integration
                energy = (self.lastPower.power + power)/2 * (now - self.lastPower.timestamp)
                if energy > 0:
                    chargedEnergy = energy
                    self.service["/History/ChargedEnergy"] += toKWh(chargedEnergy)
                    chargedAh = toAh(chargedEnergy, batteryVoltage)
                    remainingAh = min(remainingAh + chargedAh, self.config['capacity'])
                elif energy < 0:
                    dischargedEnergy = -energy
                    self.service["/History/DischargedEnergy"] += toKWh(dischargedEnergy)
                    dischargedAh = toAh(dischargedEnergy, batteryVoltage)
                    self.service["/History/TotalAhDrawn"] += dischargedAh
                    remainingAh = max(remainingAh - dischargedAh, 0)
            self.lastPower = PowerSample(power, now)

            if chargingState == FLOAT_STATE:
                remainingAh = self.config['capacity']
            self.service["/RemainingAmphours"] = remainingAh

            self.dataHistory.append(DataSample(totalCurrent, batteryVoltage, now, temperature))
            dataHistoryLen = len(self.dataHistory)
            if dataHistoryLen > MAX_DATA_HISTORY:
                del self.dataHistory[:dataHistoryLen-MAX_DATA_HISTORY]

            # median current filter
            filteredCurrentSample = sorted(self.dataHistory, key=lambda sample: sample.current)[dataHistoryLen//2]
            filteredCurrent = filteredCurrentSample.current
            # use a filtered value to remove any transients
            if filteredCurrent < 0:
                dischargeCurrent = -filteredCurrent
                self.service["/TimeToGo"] = max(round((remainingAh - DEPTH_OF_DISCHARGE/100 * self.config['capacity'])/dischargeCurrent * 3600, 0), 0)
            else:
                self.service["/TimeToGo"] = FOREVER

            soc = soc_from_voltage(compensated_voltage(batteryVoltage, temperature))
            self.service["/Soc"] = soc
            if soc < 10:
                self.service["/Alarms/LowSoc"] = ALARM_ALARM
            else:
                self.service["/Alarms/LowSoc"] = ALARM_OK
            self.service["/History/MinimumVoltage"] = _safe_min(batteryVoltage, self.service["/History/MinimumVoltage"])
            self.service["/History/MaximumVoltage"] = _safe_max(batteryVoltage, self.service["/History/MaximumVoltage"])
            deepestDischarge = self.service["/History/DeepestDischarge"]
            if deepestDischarge is None or soc < deepestDischarge:
                self.service["/History/DeepestDischarge"] = soc
            if batteryVoltage <= EMPTY_VOLTAGE:
                self.service["/History/FullDischarges"] += 1

            # median voltage filter
            filteredVoltageSample = sorted(self.dataHistory, key=lambda sample: sample.voltage)[dataHistoryLen//2]
            filteredCompensatedVoltage = compensated_voltage(filteredVoltageSample.voltage, filteredVoltageSample.temperature)
            # use a filtered value for alarm checking to remove any transients
            if filteredCompensatedVoltage <= LOW_VOLTAGE_ALARM:
                self.service["/Alarms/LowVoltage"] = ALARM_ALARM
            else:
                self.service["/Alarms/LowVoltage"] = ALARM_OK
            if filteredCompensatedVoltage >= HIGH_VOLTAGE_ALARM:
                self.service["/Alarms/HighVoltage"] = ALARM_ALARM
            else:
                self.service["/Alarms/HighVoltage"] = ALARM_OK
        return True

    def __str__(self):
        return PRODUCT_NAME


def main():
    DBusGMainLoop(set_as_default=True)
    setupOptions = Path("/data/setupOptions/BatteryProxy")
    configFile = setupOptions/"config.json"
    with configFile.open() as f:
        config = json.load(f)
    battery = BatteryService(dbusConnection(), config)
    GLib.timeout_add(250, battery.update)
    logger.info("Registered Battery Proxy")
    mainloop = GLib.MainLoop()
    mainloop.run()


if __name__ == "__main__":
    main()
