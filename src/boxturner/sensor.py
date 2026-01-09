# sensor.py
import pigpio


class SensorReader:
    def __init__(self, horizontal_pin, diagonal_pin):
        self.pi = pigpio.pi()
        # Set up bit-banged serial on both pins
        pigpio.exceptions = False
        self.pi.bb_serial_read_open(horizontal_pin, 115200)
        self.pi.bb_serial_read_open(diagonal_pin, 115200)
        pigpio.exceptions = True

    def read_data(self, pin: int) -> int:
        """
        Reads distance data from the LiDAR connected to the given GPIO pin.
        """
        results = []
        buffer = []

        while True:
            length, result = self.pi.bb_serial_read(pin)
            if length == 0:
                continue
            buffer.extend(result)

            if len(buffer) > 8:
                bytes_serial = buffer[0:9]
                if bytes_serial[0] == 0x59 and bytes_serial[1] == 0x59:
                    distance = bytes_serial[2] + bytes_serial[3] * 256
                    strength = bytes_serial[4] + bytes_serial[5] * 256
                    buffer = []
                    if distance <= 0 or distance > 300:
                        continue
                    results.append(distance)
                    if len(results) == 7:
                        mode = max(set(results), key=results.count)
                        return mode
                else:
                    buffer.pop(0)
