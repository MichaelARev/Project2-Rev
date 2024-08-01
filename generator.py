import csv
import hashlib
import random
import string
import datetime
import re

#paths for datasets and output
BASE_PATH = 'C:/Users/Hunter/Desktop/Revature/Python/Project2'
FNAME_PATH = BASE_PATH + '/datasets/new-top-firstNames.csv'
LNAME_PATH =  BASE_PATH + '/datasets/new-top-surnames.csv'
CITY_PATH = BASE_PATH + '/datasets/worldcities.csv'
PRODUCT_PATH = BASE_PATH + '/datasets/products.csv'
OUT_PATH = BASE_PATH + '/datasets/output.csv'

#creating list of first names
fnames = []
with open(FNAME_PATH, 'r') as file:
    firstnames = csv.reader(file)
    for entry in firstnames:
        fnames.append(entry[1])

#creating list of last names
lnames = []
with open(LNAME_PATH, 'r') as file:
    lastnames = csv.reader(file)
    for entry in lastnames:
        lnames.append(entry[1])

#generating list of customers with randomly generated ID [NAME, ID]
customers = []
for i in range(1000):
    #Randomly generated customer ID
    custID = ''.join([random.choice(string.ascii_letters
        + string.digits) for n in range(16)])
    #Randomly selecting a first and last name
    randIndexF = random.randint(0, len(fnames)-1)
    randIndexL = random.randint(0, len(lnames)-1)
    name = fnames[randIndexF].upper() + " " + lnames[randIndexL].upper()

    customers.append([name, custID])

#creating list of cities [CITY, COUNTRY]
cities = []
with open(CITY_PATH, 'r', encoding="utf8") as file:
    cityReader = csv.reader(file)
    for entry in cityReader:
        cities.append([entry[0], entry[4]])

#creating product list of products: [NAME, CATEGORY, ID, PRICE]
products = []
with open(PRODUCT_PATH, 'r', encoding="utf8") as file:
    hash = hashlib.sha1()
    productReader = csv.reader(file)
    i = 0
    for entry in productReader:
        if(i == 0):
            i = 1 
            continue
        i+=1
        #Dataset doesn't include product ID - generating one by hashing the product name
        hash.update(entry[1].encode())
        id = str(hash.hexdigest())
        price = re.sub('[^0-9.]','', entry[9])
        if price is not '': price = round(float(price)*0.012, 2)
        else: continue
        products.append([entry[1], entry[2], id, price])

#Generating list of random products
with open(OUT_PATH, 'w+', encoding="utf8", newline = '') as file:
    out = csv.writer(file)
    headers = ['order_id', 'customer_id', 'customer_name', 'product_id', 'product_name', 'product_category', 'payment_type', 'qty', 'price', 'datetime', 'country', 'city', 'ecommerce_website_name', 'payment_txn_id', 'payment_txn_success', 'failure reason']
    out.writerow(headers)
    for i in range(10000):
        #Randomly generated order ID
        orderID = ''.join([random.choice(string.ascii_letters
            + string.digits) for n in range(24)])
        
        #Randomly selecting customer
        randIndexC = random.randint(0, len(customers)-1)
        customer = customers[randIndexC]

        #Randomly selecting product
        randIndexP = random.randint(0, len(products)-1)
        product = products[randIndexP]

        #Randomly selecting payment choice from list
        payment_type = random.choice(['Card', 'Internet Banking', 'UPI', 'Wallet'])

        #Random quantity between 1 and 10
        qty = random.randint(1, 10)
        
        #Random date between start (2012) and end (now)
        start = datetime.date(2012, 1, 1)
        end = datetime.datetime.now().date()
        diff = end - start
        diff = diff.days
        #Selecting random date between 2012 and now
        numdays = random.randint(0, diff)
        date = start + datetime.timedelta(int(numdays))

        #randomly selecting city
        randindexC = random.randint(0, len(cities)-1)
        city = cities[randindexC]

        #Randomly selecting website from list
        website = random.choice(['www.amazon.com', 'www.ebay.com', 'www.walmart.com', 
                                 'www.wayfair.com', 'www.apple.com', 'www.homedepot.com', 
                                 'www.costco.com', 'www.alibaba.com', 'www.bestbuy.com', 
                                 'www.nike.com', 'www.etsy.com'])

        paymentID = ''.join([random.choice(string.ascii_letters
            + string.digits) for n in range(16)])
        
        #Generating transaction success - 1 in 20 transactions will decline
        roll = random.randint(1, 20)
        if(roll == 1 or product[3] == ''): 
            success = 'N'
            #selecting random failure reason from list
            reason = random.choice(['Card Declined', 'Fraud Alert', 'Insufficient Funds', 'Expired Card', 'Invalid Billing Information'])
        else:
            success = 'Y'
            reason = ""
        #generating row from randomly generated values
        row = [orderID, customer[1], customer[0], 
               product[2], product[0], product[1], 
               payment_type, qty, product[3], date,
               city[1], city[0], website, paymentID,
                success, reason]
        
        #corrupting ~5 percent of entries with either 'CORRUPTED' or empty field
        corruption = random.randint(1, 100)
        if (corruption <= 5):
            corruptedField = random.randint(0, 15)
            rand = ''.join([random.choice(string.ascii_letters
                + string.digits) for n in range(8)])
            corruptionType = random.choice(['CORRUPTED', None, rand])
            row[corruptedField] = corruptionType

        out.writerow(row)


        






