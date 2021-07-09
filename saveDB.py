import sqlite3
import pandas as pd
from pandas_datareader import data as web

class SaveDB:
    def __init__(self, file) -> None:
        self.conn = sqlite3.connect(file)
        self.cursor = self.conn.cursor()

    def create(self):
        table = '''
            CREATE TABLE IF NOT EXISTS teste(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ITSA REAL NOT NULL,
                ENBR REAL NOT NULL,
                BBSE REAL NOT NULL
            );
        '''
        self.cursor.execute(table)
    
    def insert(self, id, itsa, enbr, bbse):
        consult = 'INSERT INTO teste VALUES (?, ?, ?, ?)'

        self.cursor.execute(consult, (id, itsa, enbr, bbse))
        self.conn.commit()

    def select(self):
        self.cursor.execute('select * from teste')

        for linha in self.cursor.fetchall():
            print(f'id = {linha[0]}\nitsa = {linha[1]}\nenbr = {linha[2]}\n bbse = {linha[3]}')

    def close(self):
        self.cursor.close()
        self.conn.close()



if __name__ == "__main__":
    stocks = ['ITSA4.SA', 'ENBR3.SA', 'BBSE3.SA']
    prices = pd.DataFrame()
    for s in stocks:
        prices[s] = web.get_data_yahoo(s, '2021-01-01')['Adj Close']

    save = SaveDB('stocks.db')
    save.create()

    cont = 1
    for i in prices.values:
        save.insert(cont, i[0], i[1], i[2])
        cont += 1


    save.select()

    save.close()