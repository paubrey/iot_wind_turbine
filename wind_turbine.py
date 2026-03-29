#!/usr/bin/env python3
from gpiozero import DigitalOutputDevice, PWMOutputDevice
import time
from ADCDevice import *

motoRPin1 = DigitalOutputDevice(27)
motoRPin2 = DigitalOutputDevice(17)
enablePin = PWMOutputDevice(22, frequency=1000)
adc = ADCDevice()

def setup():
    global adc
    if(adc.detectI2C(0x48)):
        adc = PCF8591()
    elif(adc.detectI2C(0x4b)):
        adc = ADS7830()
    else:
        print("No correct I2C address found, \n"
        "Please use command 'i2cdetect -y 1' to check the I2C address! \n"
        "Program Exit. \n");
        exit(-1)

def mapNUM(value, fromLow, fromHigh, toLow, toHigh):
    return (toHigh-toLow)*(value-fromLow) / (fromHigh-fromLow) + toLow

def motor(ADC):
    value = ADC - 128
    if (value > 0):
        motoRPin1.on()
        motoRPin2.off()
        print('Turn Forward...')
        print('Because the ADC value is positive...' + str(value))
    elif (value < 0):
        motoRPin1.off()
        motoRPin2.on()
        print('Turn Backward...')
        print('Because the ADC value is negative...' + str(value))
    else:
        motoRPin1.off()
        motoRPin2.off()
        print('Motor Stop...')
    b = mapNUM(abs(value), 0, 128, 0, 100)
    enablePin.value = b / 100.0
    print('The PWM duty cycle is %d%%\n' % (abs(value)*100/127))

def loop():
    while True:
        motor_value = adc.analogRead(0)
        print('Motor ADC Value : %d' % (motor_value))
        motor(motor_value)

        light_value = adc.analogRead(1)
        voltage = light_value / 255.0 * 3.3
        print('Photoresistor ADC Value : %d, Voltage : %.2f' % (light_value, voltage))
        print('---')
        time.sleep(0.5)

def destroy():
    motoRPin1.close()
    motoRPin2.close()
    enablePin.close()
    adc.close()

if __name__ == '__main__':
    print('Program is starting ... ')
    setup()
    try:
        loop()
    except KeyboardInterrupt:
        destroy()
        print("Ending program")
