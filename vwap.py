import pandas as pd
import os
import pickle

vwap_path = '/home/vwap/'

VIOP = [
    'FAKBNK', 'FALKIM', 'FARCLK', 'FASELS', 'FBIMAS', 'FCCOLA', 'FDOHOL', 'FECILC', 'FEKGYO', 'FENJSA', 'FEREGL',
    'FFROTO', 'FGARAN', 'FGUBRF', 'FHALKB', 'FHEKTS', 'FISCTR', 'FISFIN', 'FKARSN', 'FKCHOL', 'FKOZAA', 'FKOZAL',
    'FKRDMD', 'FMGROS', 'FMPARK', 'FODAS', 'FOYAKC', 'FPETKM', 'FPGSUS', 'FSAHOL', 'FSASA', 'FSISE', 'FSKBNK', 'FSOKM',
    'FTAVHL', 'FTCELL', 'FTHYAO', 'FTKFEN', 'FTOASO', 'FTRGYO', 'FTSKB', 'FTTKOM', 'FTUPRS', 'FTURSG', 'FULKER',
    'FVAKBN', 'FVESTL', 'FYKBNK',
    'FAEFES',
    'FUSDTRY', 'FXU030',
]

BIST100 = [
    "YYLGD", "ALKIM", "KOZAL", "TTKOM", "VESTL", "TRGYO", "TCELL", "PETKM", "SISE", "CEMTS", "GOZDE", "MGROS", "ENKAI",
    "ISMEN", "NUGYO", "TSKB", "HALKB", "AKSEN", "YKBNK", "TMSN", "ALGYO", "DOAS", "GENIL", "VAKBN", "DOHOL", "SKBNK",
    "AKBNK", "ISGYO", "KARSN", "GARAN", "TTRAK", "KOZAA", "BIMAS", "TKFEN", "FROTO", "KARTN", "TOASO", "TUPRS", "KORDS",
    "DEVA", "ODAS", "LOGO", "ERBOS", "GUBRF", "KRDMD", "GLYHO", "ALBRK", "TURSG", "ECILC", "GSDHO", "PRKAB", "YATAS",
    "TSPOR", "ASELS", "CIMSA", "SNGYO", "KCHOL", "AGHOL", "AEFES", "TUKAS", "TAVHL", "SASA", "PGSUS", "IPEKE", "ULKER",
    "EGEEN", "CCOLA", "BRYAT", "AKFGY", "BAGFS", "OTKAR", "EREGL", "SELEC", "ARCLK", "ISCTR", "BUCIM", "EKGYO", "SAHOL",
    "JANTS", "THYAO", "ALARK", "HEKTS", "VESBE", "AKSA", "BERA", "NTHOL", "ISFIN", "QUAGR", "ENJSA", "AYDEM", "OYAKC",
    "MAVI", "KONTR", "SMRTG", "PSGYO", "ISDMR", "GESAN", "GWIND", "SOKM", "BASGZ"
]


def calculate_vwap(period='05', rolling=10):
    vwap_dict = {}
    print("Calculating VWAP for all stocks. period:", period, "rolling_day:", rolling)
    portfolio = VIOP + BIST100
    not_calculated = []
    for stock in portfolio:
        try:
            df = get_data(stock, period=period)
            df = df[~df.index.duplicated(keep='first')]
            df = df.resample('5min').ffill()
            df = df.between_time('09:30:00',
                                 '18:05:00')  # VIOP girince 09:30'dan başlattım, BIST için sorun olmaz zaten

            day_data = get_data(stock, period="B")  # Daily data
            day_data = day_data[~day_data.index.duplicated(keep='first')]

            df = df.tail(1200)  # (9hours*60mins/5mins)*10days = 1080 bars
            times = pd.to_datetime(df.index)
            avg_intra_volume = df.groupby([times.hour, times.minute]).volume.apply(lambda x: x.tail(rolling).mean())

            avg_intra_volume.index.names = ["hour", "minute"]
            avg_intra_volume = avg_intra_volume.to_frame()
            avg_intra_volume.columns = ["avg_intra_volume"]

            avg_dvol = float(day_data['volume'].rolling(rolling).mean().tail(1))
            avg_intra_volume["avg_day_volume"] = avg_dvol

            avg_intra_volume["avg_pct"] = avg_intra_volume["avg_intra_volume"] / avg_intra_volume["avg_day_volume"]
            vwap_dict[stock] = avg_intra_volume  # there are 10*60/5 = 120 recods in a day

        except FileNotFoundError as fe:
            not_calculated.append(stock)
            continue
        except Exception as e:
            print(stock, e)
            not_calculated.append(stock)
            continue

    vwap = pd.concat(vwap_dict, axis=1, sort=True)
    vwap.fillna(method='ffill', inplace=True)
    vwap.fillna(0, inplace=True)

    if not os.path.exists(vwap_path):
        os.makedirs(vwap_path)
    pickle.dump(vwap, open(vwap_path + "vwap_" + period + ".pickle", "wb"))

    if len(not_calculated) > 0:
        print("Exception while calculating VWAP for stock: ", not_calculated)
    print("Calculating VWAP for all stocks ended")
    return vwap
