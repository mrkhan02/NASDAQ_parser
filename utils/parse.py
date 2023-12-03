import gzip
import struct
import datetime
import pandas as pd
import os


class ITCH:

    def __init__(self,fileName):
        self.temp = []
        self.flag = None
        self.fileName=fileName
        self.bin_data=gzip.open(os.path.join('.', 'data', fileName+".gz"), 'rb')
        if not os.path.exists(os.path.join('.', fileName)):
            os.makedirs(os.path.join('.', fileName))

    def getBinary(self, size):
        read = self.bin_data.read(size)
        return read

    def convertTime(self, stamp):
        time = datetime.datetime.fromtimestamp(stamp / 1e9)
        time = time.strftime('%H:%M:%S')
        return time

    def calVWAP(self, df):
        df['Amount'] = df['Price'] * df['Volume']
        df['Time'] = pd.to_datetime(df['Time'], format='%H:%M:%S')
        df = df.groupby([df['Time'].dt.hour, df['Symbol']])[['Amount', 'Volume']].sum().reset_index()
        df['VWAP'] = df['Amount'] / df['Volume']
        df['VWAP'] = df['VWAP'].round(2)
        df['Time'] = df.apply(lambda x: str(x['Time']) + ':00:00', axis=1)
        df = df[['Time', 'Symbol', 'VWAP']]
        return df

    def getVWAP(self, message):
        parsed_data, hour = self.tradeMessage(message)
        print(parsed_data, hour)
        if self.flag is None:
            self.flag = hour
        if self.flag != hour:
            df = pd.DataFrame(self.temp, columns=['Time', 'Symbol', 'Price', 'Volume'])
            result = self.calVWAP(df)
            result.to_csv(os.path.join('.', self.fileName, str(self.flag) + '.txt'), sep=' ', index=False, escapechar='\\')
            print(result)
            self.temp = []
            self.flag = hour
        self.temp.append(parsed_data)

    def tradeMessage(self, msg):
        msg_type = b'P'
        temp = struct.unpack('>4s6sQcI8cIQ', msg)
        new_msg = struct.pack('>s4s2s6sQsI8sIQ', msg_type, temp[0], b'\x00\x00', temp[1], temp[2], temp[3], temp[4],
                      b''.join(list(temp[5:13])), temp[13], temp[14])
        value = struct.unpack('>sHHQQsI8sIQ', new_msg)
        value = list(value)
        value[3] = self.convertTime(value[3])
        value[7] = value[7].strip().decode('utf-8', errors='replace')
        value[8] = float(value[8])
        value[8] = value[8] / 10000
        return [value[3], value[7], value[8], value[6]], value[3].split(':')[0]

    def parse(self):
        msg_header = self.bin_data.read(1)
        while msg_header:
            try:
                if msg_header == b'S':
                    message = self.getBinary(11)

                elif msg_header == b'R':
                    message = self.getBinary(38)

                elif msg_header == b'H':
                    message = self.getBinary(24)

                elif msg_header == b'Y':
                    message = self.getBinary(19)

                elif msg_header == b'L':
                    message = self.getBinary(25)

                elif msg_header == b'V':
                    message = self.getBinary(34)

                elif msg_header == b'W':
                    message = self.getBinary(11)

                elif msg_header == b'K':
                    message = self.getBinary(27)

                elif msg_header == b'A':
                    message = self.getBinary(35)

                elif msg_header == b'F':
                    message = self.getBinary(39)

                elif msg_header == b'E':
                    message = self.getBinary(30)

                elif msg_header == b'C':
                    message = self.getBinary(35)

                elif msg_header == b'X':
                    message = self.getBinary(22)

                elif msg_header == b'D':
                    message = self.getBinary(18)

                elif msg_header == b'U':
                    message = self.getBinary(34)

                elif msg_header == b'P':
                    message = self.getBinary(43)
                    self.getVWAP(message)

                elif msg_header == b'Q':
                    message = self.getBinary(39)

                elif msg_header == b'B':
                    message = self.getBinary(18)

                elif msg_header == b'I':
                    message = self.getBinary(49)

                elif msg_header == b'N':
                    message = self.getBinary(19)
            except Exception as e:
                print(f"Error: {e}")
            msg_header = self.bin_data.read(1)

        self.bin_data.close()