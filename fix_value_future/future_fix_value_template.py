import ccxt
import json
import pandas as pd
import time
import decimal
from datetime import datetime
import pytz
import csv

### JUST FOR LEARNING PURPOSE USE AT YOUR OWN RISK !!!!! ####

# import neccessary package

import ccxt
import json
import pandas as pd
import time
import decimal
from datetime import datetime
import pytz
import csv

def read_config():
    with open('config.json') as json_file:
        return json.load(json_file)

config = read_config()

# API and Secret setting
api_key = config["apiKey"]
api_secret = config["secret"]
subaccount = config["sub_account"]

# Global Varibale Setting
account_name = config["account_name"]  # Set your account name (ตั้งชื่อ Account ที่ต้องการให้แสดงผล)
pair = config["pair"]
token_name = config["token_name"]
fix_value = config["rebalance_value"]
qoute_currency = config["qoute_currency"] # Set your prefer qoute currency
rebalance_value = config["rebalance_value"]

# Order Type
post_only = True  # Maker or Taker (วางโพซิชั่นเป็น MAKER เท่านั้นหรือไม่ True = ใช่)
order_type = "limit" # limit, market

# Exchange Details
exchange = ccxt.ftx({
    'apiKey': api_key,
    'secret': api_secret,
    'enableRateLimit': True}
)
exchange.headers = {'FTX-SUBACCOUNT': subaccount,}



# file system
tradelog_file = "tradinglog_{}.csv".format(subaccount)
trading_call_back = 5

# Rebalance Condition
min_reb_size = 5  # Minimum Rebalance Size ($)
time_sequence = [1, 3, 5, 7, 9, 11]  # Rebalancing Time Sequence (เวลาที่จะใช้ในการ Rebalance ใส่เป็นเวลาเดี่ยว หรือชุดตัวเลขก็ได้)
time_multiplier = 1

### Function Part ###

def get_time():  # เวลาปัจจุบัน
    named_tuple = time.localtime() # get struct_time
    Time = time.strftime("%m/%d/%Y, %H:%M:%S", named_tuple)
    return Time

def get_price():
    price = float(exchange.fetch_ticker(pair)['last'])
    return price

def get_ask_price():
    ask_price = float(exchange.fetch_ticker(pair)['ask'])
    return ask_price

def get_bid_price():
    bid_price = float(exchange.fetch_ticker(pair)['bid'])
    return bid_price

def get_pending_buy():
    pending_buy = []
    for i in exchange.fetch_open_orders(pair):
        if i['side'] == 'buy':
            pending_buy.append(i['info'])
    return pending_buy

def get_pending_sell():
    pending_sell = []
    for i in exchange.fetch_open_orders(pair):
        if i['side'] == 'sell':
            pending_sell.append(i['info'])
    return pending_sell

def create_buy_order():
    # Order Parameter
    types = order_type
    side = 'buy'
    size = buy_size
    price = buy_price
    exchange.create_order(pair, types, side, size, price, {'postOnly': post_only})
    
def create_sell_order():
    # Order Parameter
    types = order_type
    side = 'sell'
    size = sell_size
    price = sell_price
    exchange.create_order(pair, types, side, size, price, {'postOnly': post_only})
    
def cancel_order(order_id):
    
    order_id = order_id
    exchange.cancel_order(order_id)
    print("Order ID : {} Successfully Canceled".format(order_id))

def get_minimum_size():
    minimum_size = float(exchange.fetch_ticker(pair)['info']['minProvideSize'])
    return minimum_size

def get_step_size():
    step_size = float(exchange.fetch_ticker(pair)['info']['sizeIncrement'])
    return step_size

def get_step_price():
    step_price = float(exchange.fetch_ticker(pair)['info']['priceIncrement'])
    return step_price

def get_min_trade_value():
    min_trade_value = float(exchange.fetch_ticker(pair)['info']['sizeIncrement']) * price
    return min_trade_value

def get_wallet_details():
    wallet = exchange.privateGetWalletBalances()['result']
    return wallet

def get_total_port_value():
    token_lst = [[item['coin'],item['usdValue']] for item in wallet]
    total_port_value = 0
    
    for token in token_lst:
        if token[0] == qoute_currency or token[0] == token_name:
            asset_value = round(float(token[1]),2)
            total_port_value += asset_value
    return total_port_value

def get_asset_value():
    token_lst = [[item['coin'],item['usdValue']] for item in wallet]
    asset_value = 0
    
    for token in token_lst:
        if token[0] == token_name:
            value = round(float(token[1]),2)
            asset_value += value
    return asset_value

def get_cash():
    wallet = exchange.privateGetWalletBalances()['result']
    
    for t in wallet:
        if t['coin'] == qoute_currency:
            cash = float(t['availableWithoutBorrow'] )
    return cash

def get_position_size():
    positions = exchange.privateGetPositions()['result']
    position_size = 0
    for pos in positions:
        if pos['future'] == pair:
            position_size += float(pos['netSize'])
        
    return float(position_size)

def get_position_value():
    positions = exchange.privateGetPositions()['result']
    position_value = 0
    for pos in positions:
        if pos['future'] == pair:
            position_value += float(pos['cost'])
    
    return float(position_value)

def get_free_col():
    free_col = float(exchange.privateGetAccount()['result']['freeCollateral'])
    return free_col

def get_liquidation_price():
    positions = exchange.privateGetPositions()['result']
    liq_price = 0
    for pos in positions:
        if pos['future'] == pair:
            liq_price += float(pos['estimatedLiquidationPrice'])
    return liq_price


def buy_execute():
    # check pending buy order
    pending_buy = get_pending_buy()
    
    if pending_buy == []:
        print('Buying {} Size = {}'.format(token_name, buy_size))
        create_buy_order()
        time.sleep(3)
        pending_buy = get_pending_buy()

        if pending_buy != []:
            pending_buy_id = get_pending_buy()[0]['id']
            print('Buy Order Created Success, Order ID: {}'.format(pending_buy_id))
            print('Waiting For Buy Order To be Filled')
            print("------------------------------")
            time.sleep(10)
            pending_buy = get_pending_buy()

        if pending_buy == []:
            print("Buy order Matched or Cancelled")
            print("Updating Trade Log")
            update_trade_log(pair)
            print("------------------------------")
        else:
            print('Buy Order is not match, Resending...')
            pending_buy_id = get_pending_buy()[0]['id']
            order_id = pending_buy_id
            cancel_order(order_id)  
    else:
        pending_buy_id = get_pending_buy()[0]['id']
        print("Pending Buy Order Founded ID: {}".format(pending_buy_id))
        print('Waiting For Buy Order To be filled')
        time.sleep(10)
        
        if pending_buy == []:
            print("Buy order Matched or Cancelled")
            print("Updating Trade Log")
            update_trade_log(pair)
            print("------------------------------")
        else:    
            print("Canceling pending Order")
            order_id = pending_buy_id
            cancel_order(order_id)
            time.sleep(2)
            pending_buy = get_pending_buy()

        if pending_buy == []:
            print('Buy Order Matched or Cancelled')
        else:
            print('Buy Order is not Matched or Cancelled, Retrying')
    print("------------------------------")

def sell_execute():
    pending_sell = get_pending_sell()

    if pending_sell == []:
        print('Selling {} Size = {}'.format(token_name, sell_size))
        create_sell_order()
        time.sleep(3)
        pending_sell = get_pending_sell()

        if pending_sell != []:
            pending_sell_id = get_pending_sell()[0]['id']
            print('Sell Order Created Success, Order ID: {}'.format(pending_sell_id))
            print('Waiting For Sell Order To be filled')
            print("------------------------------")
            time.sleep(10)
            pending_sell = get_pending_sell()

        if pending_sell == []:
            print("Sell order Matched or Cancelled")
            print("Updating Trade Log")
            update_trade_log(pair)
            print("------------------------------")
        else:
            print('Sell Order is not match, Resending...')
            pending_sell_id = get_pending_sell()[0]['id']
            order_id = pending_sell_id
            cancel_order(order_id)

    else:
        pending_sell_id = get_pending_sell()[0]['id']
        print("Pending Sell Order Found")
        print('Waiting For Sell Order To be filled')
        time.sleep(10)
        
        if pending_sell == []:
            print("Sell order Matched or Cancelled")
            print("Updating Trade Log")
            update_trade_log(pair)
            print("------------------------------")
        else:
            print("Canceling pending Order")
            order_id = pending_sell_id
            cancel_order(order_id)
            time.sleep(3)
            pending_sell = get_pending_sell()

        if pending_sell == []:
            print('Sell Order Matched or Cancelled')
        else:
            print('Sell Order is not Matched or Cancelled, Retrying')
    print("------------------------------")

def get_last_trade_price(pair):
    pair = pair
    trade_history = pd.DataFrame(exchange.fetchMyTrades(pair, limit = 1),
                            columns=['id', 'timestamp', 'datetime', 'symbol', 'side', 'price', 'amount', 'cost', 'fee'])
    last_trade_price = trade_history['price']
    
    return float(last_trade_price)

# Database Function Part

def checkDB():
    try:
        tradinglog = pd.read_csv(tradelog_file)
        print('DataBase Exist Loading DataBase....')
    except:
        tradinglog = pd.DataFrame(columns=['id', 'timestamp', 'time', 'pair', 'side', 'price', 'qty', 'fee', 'timeseries', 'bot_name', 'subaccount', 'cost'])
        tradinglog.to_csv(tradelog_file, index=False)
        print("Database Created")
        
        
    return tradinglog

def create_funding_csv():
    try:      
        dffunding  = pd.read_csv("dffunding.csv")
    except:
        dffunding = pd.DataFrame(columns=['id','future','payment','time','rate'])
        dffunding.to_csv("dffunding.csv",index=False) 

# Database Setup
print('Checking Database file.....')
tradinglog = checkDB()
create_funding_csv()

def get_trade_history(pair):
    pair = pair
    trade_history = pd.DataFrame(exchange.fetchMyTrades(pair, limit = trading_call_back),
                              columns=['id', 'timestamp', 'datetime', 'symbol', 'side', 'price', 'amount', 'fee'])
    
    cost=[]
    for i in range(len(trade_history)):
        fee = trade_history['fee'].iloc[i]['cost'] if trade_history['fee'].iloc[i]['currency'] == 'USD' else trade_history['fee'].iloc[i]['cost'] * trade_history['price'].iloc[i]
        cost.append(fee)  # ใน fee เอาแค่ cost
    
    trade_history['fee'] = cost
    
    return trade_history

def get_last_id(pair):
    pair = pair
    trade_history = get_trade_history(pair)
    last_trade_id = (trade_history.iloc[:trading_call_back]['id'])
    
    return last_trade_id

def update_trade_log(pair):
    pair = pair
    tradinglog = pd.read_csv(tradelog_file)
    last_trade_id = get_last_id(pair)
    trade_history = get_trade_history(pair)
    
    for i in last_trade_id:
        tradinglog = pd.read_csv(tradelog_file)
        trade_history = get_trade_history(pair)
    
        if int(i) not in tradinglog.values:
            print("New Trade Founded")
            last_trade = trade_history.loc[trade_history['id'] == i]
            list_last_trade = last_trade.values.tolist()[0]

            # แปลงวันที่ใน record
            d = datetime.strptime(list_last_trade[2], "%Y-%m-%dT%H:%M:%S.%fZ")
            d = pytz.timezone('Etc/GMT+7').localize(d)
            d = d.astimezone(pytz.utc)
            Date = d.strftime("%Y-%m-%d")
            Time = d.strftime("%H:%M:%S")
            time_serie = (d.weekday()*1440)+(d.hour*60)+d.minute

            cost = float(list_last_trade[5] * list_last_trade[6])

            # edit & append ข้อมูลก่อน add เข้า database
            list_last_trade[1] = Date
            list_last_trade[2] = Time
            list_last_trade.append(time_serie)
            list_last_trade.append(account_name)
            list_last_trade.append(subaccount)
            list_last_trade.append(cost)

            ## list_last_trade.append(cost)

            with open(tradelog_file, "a+", newline='') as fp:
                wr = csv.writer(fp, dialect='excel')
                wr.writerow(list_last_trade)
            print('Recording Trade ID : {}'.format(i))
        else:
            print('Trade Already record')
  
def get_funding():
    df_funding = pd.DataFrame(exchange.private_get_funding_payments()['result'],
                    columns=['id','future','payment','time','rate'])
    return df_funding

def update_funding():
    funding_in_csv = pd.read_csv('dffunding.csv')
    check_id_in_csv = funding_in_csv['id'].values.tolist()
    funding_ftx = get_funding()
    for i in range(len(funding_ftx)):
        if int(funding_ftx['id'][i]) not in check_id_in_csv :
            with open("dffunding.csv", "a+", newline='') as fp:
                wr = csv.writer(fp, dialect='excel')
                wr.writerow(funding_ftx.iloc[i])

while True:
    try:
        print('Validating Trading History')
        update_trade_log(pair)
        print("------------------------------")
        print('Validating History Finished')
        print("------------------------------")

        wallet = get_wallet_details()
        cash = get_cash()
        Time = get_time()
        free_col = get_free_col()
        asset_value = get_position_value()
        asset_size = get_position_size()

        # Checking Initial Balance Loop
        while asset_value < 1 and free_col > 0:
            print('Entering Initial Loop')
            print("------------------------------")
            wallet = get_wallet_details()
            cash = get_cash()
            Time = get_time()
            free_col = get_free_col()
            asset_value = get_position_value()
            print('Time : {}'.format(Time))
            print('Account : {}'.format(account_name))
            print('Your Remaining Cash : {} {}'.format(cash, qoute_currency))
            print('Your free collateral is {}'.format(free_col))
            print('Your {} value: {} USD'.format(token_name, asset_value))
            print('Your {} size: {}'.format(token_name, asset_size, token_name))
            print("------------------------------")
            print('{} is missing'.format(token_name))
    
            # Get price params
            price = get_price()
            ask_price = get_ask_price()
            bid_price = get_bid_price()

            # Innitial asset BUY params
            min_size = get_minimum_size()
            step_price = get_step_price()
            min_trade_value = get_min_trade_value()
            cash = get_cash()
            pending_buy = get_pending_buy()

            # Create BUY params
            initial_diff = rebalance_value - asset_value
            buy_size = initial_diff / price
            buy_price = bid_price

            print('Checking {} Buy Condition ......'.format(format(token_name)))
            if free_col > min_trade_value and buy_size > min_size:
                buy_execute()
            elif free_col < min_trade_value:
                print("Not Enough Collateral to buy {}".format(token_name))
                print('Your Collateral is {} // Minimum Trade Value is {}'.format(free_col, min_trade_value))
            else:    
                print("Buy size is too small")
                print("Your {} order size is {} but minimim size is {}".format(token_name, buy_size, min_size))
                print("------------------------------")

        # Rebalancing Loop
        while asset_value > 1 and free_col > 0:
            print('Entering Rebalance Loop')
            print("------------------------------")
            for t in time_sequence:
                print('Current Time Sequence is : {}'.format(t))
                print("------------------------------")
                wallet = get_wallet_details()
                cash = get_cash()
                Time = get_time()
                
            
                # Rebalancing Parameter check
                price = get_price()
                asset_value = get_position_value()
                free_col = get_free_col()
                fixed_value = rebalance_value
                diff = abs(fixed_value - asset_value)
                last_trade_price = get_last_trade_price(pair)
                liq_price = get_liquidation_price()

                print('Time : {}'.format(Time))
                print('Account : {}'.format(account_name))
                print('Your Remaining Cash : {} {}'.format(cash, qoute_currency))
                print('Your free collateral is {}'.format(free_col))
                print('Your Liquidation Price is : {}'.format(liq_price))
                print('{} Price is {}'.format(token_name, price))
                print('Your {} Value is {}'.format(token_name, asset_value))
                print('Diff = {}'.format(str(diff)))
                print('Last trade price is {}'.format(last_trade_price))
        
                if asset_value < fixed_value - min_reb_size and price < last_trade_price:
                    print("Current {} Value less than fix value : Rebalancing -- Buy".format(token_name))

                    # Recheck trading params
                    price = get_price()
                    bid_price = get_bid_price()
                    ask_price = get_ask_price()
                    min_size = get_minimum_size()
                    step_price = get_step_price()
                    min_trade_value = get_min_trade_value()
                    cash = get_cash()
                    asset_value = get_position_value()
                    diff = abs(fixed_value - asset_value)
                    pending_buy = get_pending_buy()

                    # Create BUY params
                    buy_size = diff / price
                    buy_price = bid_price
            
                    # BUY order execution
                    if free_col > min_trade_value and buy_size > min_size:
                        buy_execute()
                    elif free_col < min_trade_value:
                        print("Not Enough Collateral to buy {}".format(token_name))
                        print('Your Collateral is {} // Minimum Trade Value is {}'.format(free_col, min_trade_value))
                    else:
                        print("Buy size is too small")
                        print("Your order size is {} but minimim size is {}".format(str(buy_size, min_size)))
                        print("------------------------------")
                
                elif asset_value > fixed_value + min_reb_size and price > last_trade_price:
                    print("Current {} Value more than fix value : Rebalancing -- Sell".format(token_name))
                                
                    # Recheck trading params
                    price = get_price()
                    bid_price = get_bid_price()
                    ask_price = get_ask_price()
                    min_size = get_minimum_size()
                    step_price = get_step_price()
                    min_trade_value = get_min_trade_value()
                    cash = get_cash()
                    asset_value = get_position_value()
                    diff = abs(fixed_value - asset_value)
                    pending_sell = get_pending_sell()
                                        
                    # Create SELL params
                    sell_size = diff / price
                    sell_price = ask_price
                                
                    # SELL order execution
                    if diff > min_trade_value and sell_size > min_size:
                        sell_execute()
                    else:
                        print("Not Enough Balance to sell {}".format(token_name))
                        print('You have {} {} // Minimum Trade Value is {}'.format(token_name, asset_value, min_trade_value))
                        print("------------------------------")
                                
                else:
                    print("Current {} Value is not reach fix value yet : Waiting".format(token_name))
                    print("------------------------------")
                    time.sleep(5)

                # Rebalancing Time Sequence
                time.sleep(t * time_multiplier)

    except Exception as e:
        print('Error : {}'.format(str(e)))
        time.sleep(10)  