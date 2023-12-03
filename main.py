from utils.parse import ITCH


if __name__ == '__main__':
    fileName="01302019.NASDAQ_ITCH50"
    itch = ITCH(fileName=fileName)
    itch.parse()