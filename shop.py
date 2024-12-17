import requests
import json
import time
import pandas as pd
import logging
import datetime as dt
import numpy as np
import os

import TelegramNotifier
import DatabaseHandler
import LoggingHandler


telegram_notifier = TelegramNotifier.Notifier()
db_handler = DatabaseHandler.Handler()
auth_table = "auth_table" # name of the Authentication table

error_logger,info_logger = LoggingHandler.Handler()

# TODO: use apscheduler
while True:
    try:        
        # Get all orders from the api service of the shop
        k = "key" # apiKey
        url = "https://api.shop.com/v1/orders?limit=50&sort=dateDesc" # URL of the shop
        headers = {"accept": "application/json","authorization": "Bearer " + k} # Arrange the headers
        
        # Api request
        response = requests.get(url, headers=headers) 
        
        if response.status_code != 200:
            log = f"API error. Couldn't fetch last orders . response_code = {response.status_code}  ||  reason: {response.reason}  || " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            print(log)
            error_logger.warning(log)
            continue
        
        else:
            orders = json.loads(response.text)
        
        fulfilled_accounts = 0 # Used for logging

        # Check all the last orders to see if they are fulfilled or not                
        for order in orders:               
            status = "none"
            order_successful = False
            membership_given = False
            try:
                payment_status = order['paymentStatus'] # == 'paid' if the payment if recieved
                order_id = order['id'] # ORder ID
                
                # Payment recieved
                if payment_status == "paid":                
                    note = order['note'] # TELEGRAM ID is asked to customer in the "Notes" section of order. 
                    status = order['status'] # default value is unfulfilled. Will change it to fulfilled after membership is given
                    currency = order["currency"] # paid currency
                    date = order["dateCreated"] # creation date of the order
                    discounts = order["discounts"] # check for discounts
                    email = order["shippingInfo"]["email"] # e-mail
                    odeme_miktari = order['totals']['total'] # paid amount
                    
                    # Will fill these later
                    yeni_uyelik = False     
                    eski_uyelik_tarihi_dolmus = False
                    previous_membership = []
                    
                    order_successful = True # == True, if there are no problems with the order
                
                # Payment is NOT recieved yet
                else:
                    order_successful = False
                    vakit =  str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    log = f"{order_id} Payment is not yet recieved. Payment Status: {payment_status} Time: {vakit}."
                    print(log)
                    info_logger.info(log)
            
            # Error handling
            except Exception as e:
                order_successful = False
                time_temp =  str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                error_log = f"Error while checking the order -- {time_temp} -- {e}"
                print(error_log)                
                error_logger.warning(error_log)
                
            
            # Order is already processed and fulfilled
            if order_successful and status == "fulfilled":
                fulfilled_accounts += 1 
                # print(f"Order ID: {order_id} | User: {note} | Purch. Date: {date}  | ALREADY FULFILLED | " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            
            # Payment is recieved. Order is not fulfilled
            if status == "unfulfilled" and payment_status == "paid" and order_successful == True:
                try:
                    time_temp =  str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    log = f"Giving membership to Telegram ID: {note}. Recieved payment amount: {odeme_miktari} | Date: {date} | E-mail: {email}"
                    print(log)
                    info_logger.info(log)
                    
                    # We will check the database to see if this is a new membership, or did this user had a membership before
                    previous_membership_id,previous_membership_username = [],[]
                    sql_str_mem = f"SELECT * FROM {auth_table} WHERE telegram_username = '{note}';"
                    previous_membership_username,auth_columns = db_handler.get_item(sql_str_mem)
                    
                    # If the note is all integers, then Telegram ID is used in the order. Since usernames can't be all numbers.
                    if note.isdigit(): 
                        sql_str_mem_ = f"SELECT * FROM {auth_table} WHERE telegram_id = {note};"
                        previous_membership_id,auth_columns = db_handler.get_item(sql_str_mem_)
                    
                    # No previous membership
                    if len(previous_membership_username) == 0 and len(previous_membership_id) == 0:            
                        yeni_uyelik = True # Needs to be true, if a new membership is going to be created.    
                        log = f"{note} -- No previous memberships."
                        print(log)
                        info_logger.info(log)
                    
                    # A previous membership already exists
                    else:
                        yeni_uyelik = False
                        log = f"{note} -- Previous membership exists"
                        print(log)
                        info_logger.info(log)
                        
                        if len(previous_membership_id) != 0: previous_membership = previous_membership_id # If not empty, means user gave ustheir telegram id, not username
                        else: previous_membership = previous_membership_username
                        
                        # Details of previous membership is temporarily kept in a dataframe.
                        # This consists details like expiry date, some user preferences etc.
                        previous_membership = pd.DataFrame(previous_membership[0]).T 
                        previous_membership.columns = auth_columns
                        
                        # Check for the expiry date
                        uyelik_bitis_tarihi = previous_membership['expiry'].values[0]     
                        
                        # Membership expired
                        if np.datetime64('now') >= uyelik_bitis_tarihi:
                            eski_uyelik_tarihi_dolmus = True
                        
                        # MEmbership is not expired
                        else:eski_uyelik_tarihi_dolmus = False
                        
                except Exception as e:
                    time_temp =  str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    error_log = f"{note} -- Errro while getting user data -- {time_temp} -- {e}"
                    print(error_log)
                    error_logger.warning(error_log)
                    db_handler.reset_connection()
        
                try:
                    # New membership. Assign a 1 month membership                         
                    if yeni_uyelik:
                        expiry_date = (dt.datetime.now() + dt.timedelta(days=31)).strftime("%Y-%m-%d %H:%M:%S") # Calculate the expiry date
                        membership = 1 # Default membership value is 1 for this app
                        sql_str = ""
                        
                        # User gave us their Telegram id. We will use that to give membership     
                        # Also, we will save some information about this order, in the "last_orders" column.
                        if note.isdigit():                
                            sql_str = f"""
                            INSERT INTO {auth_table} (telegram_id, expiry, membership, last_orders)
                            VALUES (
                                {note}, 
                                '{expiry_date}', 
                                {membership}, 
                                JSON_ARRAY(
                                    JSON_OBJECT('order_id', '{order_id}', 'date', '{date}', 'odeme_miktari', '{odeme_miktari}' , 'currency', '{currency}', 'email', '{email}')
                                )
                            );
                            """
                            
                        # Give memership using the username
                        else:
                            sql_str = f"""
                            INSERT INTO {auth_table} (telegram_username, expiry, membership, last_orders)
                            VALUES (
                                '{note}', 
                                '{expiry_date}', 
                                {membership}, 
                                JSON_ARRAY(
                                    JSON_OBJECT('order_id', '{order_id}', 'date', '{date}', 'odeme_miktari', '{odeme_miktari}' , 'currency', '{currency}', 'email', '{email}')
                                )
                            );
                            """                        
                        
                        changed_rows = db_handler.genel_sql(sql_str)
                        if changed_rows >= 1: 
                            vakit =  str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                            log = f"SUCCESSFULLY GAVE MEMBERSHIP TO: {note} {vakit}. Odenen Tutar: {odeme_miktari} | Date: {date} | E-mail: {email}"
                            print(log)
                            info_logger.info(log)
                            membership_given = True
                            
                    
                    # Previous membership exists. We will check if for the expiry dates to determine how much we need to extend this membership.
                    if not yeni_uyelik and len(previous_membership) > 0: 
                        # We will check if we processed this order_id or not. This prevents us from giving more than 1 memberships per payment.
                        yeni_siparis = True # Will be false, if we already processed this order_id                 
                        
                        if previous_membership['last_orders'].isnull().values[0]: last_orders = []; yeni_siparis = True  # Last orders column empty
                        
                        else: # Last orders is not empty. We will check the orders_ids
                            last_orders = json.loads(previous_membership['last_orders'].values[0])
                            for last_order in last_orders:
                                if str(last_order['order_id']) == order_id:
                                    yeni_siparis = False
                                    vakit =  str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                                    log = f"{note} -- {order_id} -- This order is already processed. {vakit}"
                                    print(log)
                                    info_logger.info(log)
                                    membership_given = True
                        
                        # New order
                        if yeni_siparis:
                            # User's membership is already expired. Calculating the new expiry date (1 month)
                            if eski_uyelik_tarihi_dolmus:
                                expiry_date = (dt.datetime.now() + dt.timedelta(days=31)).strftime("%Y-%m-%d %H:%M:%S") # Calculate expiry date
                                membership = 1
                            
                            # User's membership is NOT expired. Extending it for 1 month.
                            else:    
                                uyelik_bitis_tarihi = previous_membership['expiry'].values[0]   
                                uyelik_bitis_tarihi = np.datetime64(uyelik_bitis_tarihi)
                                uyelik_bitis_tarihi = (uyelik_bitis_tarihi - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')
                                uyelik_bitis_tarihi = dt.datetime.fromtimestamp(int(uyelik_bitis_tarihi)) # Timestamp to datetime
                                expiry_date = (uyelik_bitis_tarihi + dt.timedelta(days=31)).strftime("%Y-%m-%d %H:%M:%S") # Calculte expiry date
                                membership = 1
                            
                            # Update the membership.
                            if str(note).isdigit():           
                                sql_str = f"""
                                UPDATE AAA_TETABOT_AUTH
                                SET 
                                    expiry = '{expiry_date}',
                                    last_orders = JSON_ARRAY_APPEND(IFNULL(last_orders, JSON_ARRAY()), '$',  JSON_OBJECT('order_id', '{order_id}', 'date', '{date}', 'odeme_miktari', '{odeme_miktari}' , 'currency', '{currency}', 'email', '{email}'))
                                WHERE telegram_id = {note};
                                """
                            else:
                                sql_str = f"""
                                UPDATE AAA_TETABOT_AUTH
                                SET 
                                    expiry = '{expiry_date}',
                                    last_orders = JSON_ARRAY_APPEND(IFNULL(last_orders, JSON_ARRAY()), '$', JSON_OBJECT('order_id', '{order_id}', 'date', '{date}', 'odeme_miktari', '{odeme_miktari}' , 'currency', '{currency}', 'email', '{email}'))
                                WHERE telegram_username = '{note}';
                                """         
                                
                            changed_rows = db_handler.genel_sql(sql_str)
                            if changed_rows >= 1:          
                                vakit =  str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                                log = f"{note} icin uyelik UZATILMASI basariyla verildi Su anki zaman: {vakit}. | Yeni bitis Tarihi: {expiry_date } | Odenen Tutar: {odeme_miktari} | Date: {date} | E-mail: {email}"
                                print(log)
                                info_logger.info(log)
                            

                except Exception as e:
                    vakit =  str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    error_log = f"{note} -- Error while updating user data -- {vakit} -- {e}"
                    print(error_log)
                    error_logger.warning(error_log)
                        
                        
                # Membership processing is succesful
                # We will change the order status in the shop to "Fulfilled"
                if membership_given:
                    try:
                        for i in range(0,10): # 10 tries
                            url = f"https://api.shop.com/v1/orders/{order_id}"
        
                            payload = { "fulfillments": {
                                    "productType": "digital",
                                    "note": "1 aylik tetabot erisimi tanimlandi"
                                } }
                            headers = {
                                "accept": "application/json",
                                "content-type": "application/json",
                                "authorization": "Bearer " + k
                            }            
                            
                            response = requests.put(url, json=payload, headers=headers)
                        
                        if response.status_code == 200:
                            vakit =  str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                            log = f"{order_id} - FULFILLED -  Tarih: {vakit}"
                            print(log)
                            info_logger.info(log)
                            break
                    
                    except Exception as e:
                        vakit =  str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        error_log = f"{order_id} Error whgile fulfilling the order username or id: {note} -- {vakit} -- {e}"
                        print(error_log)
                        error_logger.warning(error_log)
            
        
                    try:
                        text = "Order is processed " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))+ f"\nSiparis Notu: {note}"+ f"\nMiktar: {odeme_miktari} {currency}"
                        
                        # Send a telegram message to admins to notify the successful order processing.
                        telegram_notifier.send_message(text)
                            
                    except Exception as e:
                        vakit =  str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        error_log = f"Error -- Could not send Telegram messages -- {vakit} -- {e}"
                        print(error_log)
                        error_logger.warning(error_log)
        
        print(f"{fulfilled_accounts} ORDERS ALREADY FULFILLED | " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        time.sleep(5)
        
    except Exception as e:
        vakit = str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print("Main loop error -- ",vakit," -- ",e)
        error_logger.warning("Main loop error -- " + vakit + " -- " + str(e))
        

 