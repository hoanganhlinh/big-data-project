import findspark
findspark.init()

from pyspark.sql import SparkSession
import pandas as pd
import happybase as hb
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
pd.DataFrame.iteritems = pd.DataFrame.items

# print("\n\n\n\n\n\n\n\n\n\n\n\n")

def load_hbase(table_name):
    con = hb.Connection('localhost')
    table = con.table(table_name)
    rows = table.scan()
    data = [{k.decode().split(' ')[-1]: v.decode() for k, v in r[1].items()} for r in rows]
    con.close()
    df = pd.DataFrame(data)
    return df

##################### CURRENCY #####################

df_currency = load_hbase(b'currency_rates')
print("-------- df_currency --------")
print(df_currency.head(3))
print("-----------------------------")


print("\n\n\n\n")
print("-------------------------------- SPARK --------------------------------")
spark = SparkSession.builder.appName("Spark").getOrCreate()
df = spark.createDataFrame(df_currency)

df=df.withColumnRenamed("Metadata:date", "date")
df=df.withColumnRenamed("Metadata:base", "base")
df=df.withColumnRenamed("Metadata:timestamp_currency", "timestamp")
df=df.withColumnRenamed("Rates:AED", "AED")
df=df.withColumnRenamed("Rates:AFN", "AFN")
df=df.withColumnRenamed("Rates:ALL", "ALL")
df=df.withColumnRenamed("Rates:AMD", "AMD")
df=df.withColumnRenamed("Rates:ANG", "ANG")
df=df.withColumnRenamed("Rates:AOA", "AOA")
df=df.withColumnRenamed("Rates:ARS", "ARS")
df=df.withColumnRenamed("Rates:AUD", "AUD")
df=df.withColumnRenamed("Rates:AWG", "AWG")
df=df.withColumnRenamed("Rates:AZN", "AZN")
df=df.withColumnRenamed("Rates:BAM", "BAM")
df=df.withColumnRenamed("Rates:BBD", "BBD")
df=df.withColumnRenamed("Rates:BDT", "BDT")
df=df.withColumnRenamed("Rates:BGN", "BGN")
df=df.withColumnRenamed("Rates:BHD", "BHD")
df=df.withColumnRenamed("Rates:BIF", "BIF")
df=df.withColumnRenamed("Rates:BMD", "BMD")
df=df.withColumnRenamed("Rates:BND", "BND")
df=df.withColumnRenamed("Rates:BOB", "BOB")
df=df.withColumnRenamed("Rates:BRL", "BRL")
df=df.withColumnRenamed("Rates:BSD", "BSD")
df=df.withColumnRenamed("Rates:BTC", "BTC")
df=df.withColumnRenamed("Rates:BTN", "BTN")
df=df.withColumnRenamed("Rates:BWP", "BWP")
df=df.withColumnRenamed("Rates:BYN", "BYN")
df=df.withColumnRenamed("Rates:BYR", "BYR")
df=df.withColumnRenamed("Rates:BZD", "BZD")
df=df.withColumnRenamed("Rates:CAD", "CAD")
df=df.withColumnRenamed("Rates:CDF=DF", "CDF=DF")
df=df.withColumnRenamed("Rates:CHF", "CHF")
df=df.withColumnRenamed("Rates:CLF", "CLF")
df=df.withColumnRenamed("Rates:CLP", "CLP")
df=df.withColumnRenamed("Rates:CNY", "CNY")
df=df.withColumnRenamed("Rates:COP", "COP")
df=df.withColumnRenamed("Rates:CRC", "CRC")
df=df.withColumnRenamed("Rates:CUC", "CUC")
df=df.withColumnRenamed("Rates:CUP", "CUP")
df=df.withColumnRenamed("Rates:CVE", "CVE")
df=df.withColumnRenamed("Rates:CZK", "CZK")
df=df.withColumnRenamed("Rates:DJF", "DJF")
df=df.withColumnRenamed("Rates:DKK", "DKK")
df=df.withColumnRenamed("Rates:DOP", "DOP")
df=df.withColumnRenamed("Rates:DZD", "DZD")
df=df.withColumnRenamed("Rates:EGP", "EGP")
df=df.withColumnRenamed("Rates:ERN", "ERN")
df=df.withColumnRenamed("Rates:ETB", "ETB")
df=df.withColumnRenamed("Rates:EUR", "EUR")
df=df.withColumnRenamed("Rates:FJD", "FJD")
df=df.withColumnRenamed("Rates:FKP", "FKP")
df=df.withColumnRenamed("Rates:GBP", "GBP")
df=df.withColumnRenamed("Rates:GEL", "GEL")
df=df.withColumnRenamed("Rates:GGP", "GGP")
df=df.withColumnRenamed("Rates:GHS", "GHS")
df=df.withColumnRenamed("Rates:GIP", "GIP")
df=df.withColumnRenamed("Rates:GMD", "GMD")
df=df.withColumnRenamed("Rates:GNF", "GNF")
df=df.withColumnRenamed("Rates:GTQ", "GTQ")
df=df.withColumnRenamed("Rates:GYD", "GYD")
df=df.withColumnRenamed("Rates:HKD", "HKD")
df=df.withColumnRenamed("Rates:HNL", "HNL")
df=df.withColumnRenamed("Rates:HRK", "HRK")
df=df.withColumnRenamed("Rates:HTG", "HTG")
df=df.withColumnRenamed("Rates:HUF", "HUF")
df=df.withColumnRenamed("Rates:IDR", "IDR")
df=df.withColumnRenamed("Rates:ILS", "ILS")
df=df.withColumnRenamed("Rates:IMP", "IMP")
df=df.withColumnRenamed("Rates:INR", "INR")
df=df.withColumnRenamed("Rates:IQD", "IQD")
df=df.withColumnRenamed("Rates:IRR", "IRR")
df=df.withColumnRenamed("Rates:ISK", "ISK")
df=df.withColumnRenamed("Rates:JEP", "JEP")
df=df.withColumnRenamed("Rates:JMD", "JMD")
df=df.withColumnRenamed("Rates:JOD", "JOD")
df=df.withColumnRenamed("Rates:JPY", "JPY")
df=df.withColumnRenamed("Rates:KES", "KES")
df=df.withColumnRenamed("Rates:KGS", "KGS")
df=df.withColumnRenamed("Rates:KHR", "KHR")
df=df.withColumnRenamed("Rates:KMF", "KMF")
df=df.withColumnRenamed("Rates:KPW", "KPW")
df=df.withColumnRenamed("Rates:KRW", "KRW")
df=df.withColumnRenamed("Rates:KWD", "KWD")
df=df.withColumnRenamed("Rates:KYD", "KYD")
df=df.withColumnRenamed("Rates:KZT", "KZT")
df=df.withColumnRenamed("Rates:LAK", "LAK")
df=df.withColumnRenamed("Rates:LBP", "LBP")
df=df.withColumnRenamed("Rates:LKR", "LKR")
df=df.withColumnRenamed("Rates:LRD", "LRD")
df=df.withColumnRenamed("Rates:LSL", "LSL")
df=df.withColumnRenamed("Rates:LTL", "LTL")
df=df.withColumnRenamed("Rates:LVL", "LVL")
df=df.withColumnRenamed("Rates:LYD", "LYD")
df=df.withColumnRenamed("Rates:MAD", "MAD")
df=df.withColumnRenamed("Rates:MDL", "MDL")
df=df.withColumnRenamed("Rates:MGA", "MGA")
df=df.withColumnRenamed("Rates:MKD", "MKD")
df=df.withColumnRenamed("Rates:MMK", "MMK")
df=df.withColumnRenamed("Rates:MNT", "MNT")
df=df.withColumnRenamed("Rates:MOP", "MOP")
df=df.withColumnRenamed("Rates:MRU", "MRU")
df=df.withColumnRenamed("Rates:MUR", "MUR")
df=df.withColumnRenamed("Rates:MVR", "MVR")
df=df.withColumnRenamed("Rates:MWK", "MWK")
df=df.withColumnRenamed("Rates:MXN", "MXN")
df=df.withColumnRenamed("Rates:MYR", "MYR")
df=df.withColumnRenamed("Rates:MZN", "MZN")
df=df.withColumnRenamed("Rates:NAD", "NAD")
df=df.withColumnRenamed("Rates:NGN", "NGN")
df=df.withColumnRenamed("Rates:NIO", "NIO")
df=df.withColumnRenamed("Rates:NOK", "NOK")
df=df.withColumnRenamed("Rates:NPR", "NPR")
df=df.withColumnRenamed("Rates:NZD", "NZD")
df=df.withColumnRenamed("Rates:OMR", "OMR")
df=df.withColumnRenamed("Rates:PAB", "PAB")
df=df.withColumnRenamed("Rates:PEN", "PEN")
df=df.withColumnRenamed("Rates:PGK", "PGK")
df=df.withColumnRenamed("Rates:PHP", "PHP")
df=df.withColumnRenamed("Rates:PKR", "PKR")
df=df.withColumnRenamed("Rates:PLN", "PLN")
df=df.withColumnRenamed("Rates:PYG", "PYG")
df=df.withColumnRenamed("Rates:QAR", "QAR")
df=df.withColumnRenamed("Rates:RON", "RON")
df=df.withColumnRenamed("Rates:RSD", "RSD")
df=df.withColumnRenamed("Rates:RUB", "RUB")
df=df.withColumnRenamed("Rates:RWF", "RWF")
df=df.withColumnRenamed("Rates:SAR", "SAR")
df=df.withColumnRenamed("Rates:SBD", "SBD")
df=df.withColumnRenamed("Rates:SCR", "SCR")
df=df.withColumnRenamed("Rates:SDG", "SDG")
df=df.withColumnRenamed("Rates:SEK", "SEK")
df=df.withColumnRenamed("Rates:SGD", "SGD")
df=df.withColumnRenamed("Rates:SHP", "SHP")
df=df.withColumnRenamed("Rates:SLE", "SLE")
df=df.withColumnRenamed("Rates:SLL", "SLL")
df=df.withColumnRenamed("Rates:SOS", "SOS")
df=df.withColumnRenamed("Rates:SRD", "SRD")
df=df.withColumnRenamed("Rates:STD", "STD")
df=df.withColumnRenamed("Rates:SYP", "SYP")
df=df.withColumnRenamed("Rates:SZL", "SZL")
df=df.withColumnRenamed("Rates:THB", "THB")
df=df.withColumnRenamed("Rates:TJS", "TJS")
df=df.withColumnRenamed("Rates:TMT", "TMT")
df=df.withColumnRenamed("Rates:TND", "TND")
df=df.withColumnRenamed("Rates:TOP", "TOP")
df=df.withColumnRenamed("Rates:TRY", "TRY")
df=df.withColumnRenamed("Rates:TTD", "TTD")
df=df.withColumnRenamed("Rates:TWD", "TWD")
df=df.withColumnRenamed("Rates:TZS", "TZS")
df=df.withColumnRenamed("Rates:UAH", "UAH")
df=df.withColumnRenamed("Rates:UGX", "UGX")
df=df.withColumnRenamed("Rates:USD", "USD")
df=df.withColumnRenamed("Rates:UYU", "UYU")
df=df.withColumnRenamed("Rates:UZS", "UZS")
df=df.withColumnRenamed("Rates:VEF", "VEF")
df=df.withColumnRenamed("Rates:VES", "VES")
df=df.withColumnRenamed("Rates:VND", "VND")
df=df.withColumnRenamed("Rates:VUV", "VUV")
df=df.withColumnRenamed("Rates:WST", "WST")
df=df.withColumnRenamed("Rates:XAF", "XAF")
df=df.withColumnRenamed("Rates:XAG", "XAG")
df=df.withColumnRenamed("Rates:XAU", "XAU")
df=df.withColumnRenamed("Rates:XCD", "XCD")
df=df.withColumnRenamed("Rates:XDR", "XDR")
df=df.withColumnRenamed("Rates:XOF", "XOF")
df=df.withColumnRenamed("Rates:XPF", "XPF")
df=df.withColumnRenamed("Rates:YER", "YER")
df=df.withColumnRenamed("Rates:ZAR", "ZAR")
df=df.withColumnRenamed("Rates:ZMK", "ZMK")
df=df.withColumnRenamed("Rates:ZMW", "ZMW")
df=df.withColumnRenamed("Rates:ZWL", "ZWL")

cities = ["London", "Paris", "Warsaw", "New York", "Berlin", "Rome", "Zagreb", "Amsterdam", "Vienna", "Prague", "Madrid"]
currencies = ["GBP", "EUR", "PLN", "USD", "EUR", "EUR", "HRK", "EUR", "EUR", "CZK", "EUR"]
selected_columns = ["date", "base", "timestamp"] + [f"{currency}" for currency in currencies]
selected_columns = list(set(selected_columns))
df_selected = df.select(*selected_columns)

print("-------- df_selected --------")
df_selected.show()
print("-----------------------------")

currencies2 = list(set(currencies))
df_pandas = df_selected.toPandas()
df_pandas[currencies2] = df_pandas[currencies2].astype(float)
print(df_pandas.head(5))

fig, axes = plt.subplots(nrows=2, ncols=3, figsize=(15, 8), sharex=True)

for i, currency in enumerate(["GBP", "PLN", "USD", "HRK", "CZK"]):
    row = i // 3
    col = i % 3
    axes[row, col].plot(df_pandas['timestamp'], df_pandas[currency], label=currency)
    axes[row, col].set_ylabel('Exchange Rate')
    axes[row, col].set_title(f'Currency Exchange Rate ({currency})')
    axes[row, col].legend()

for ax in axes[-1, :]:
    ax.set_xlabel('Timestamp')
# plt.xticks([])
plt.tight_layout()
plt.show()


print("-----------------------------------------------------------------------")



##################### AIRBNB #####################

df_airbnb = load_hbase(b'airbnb')
print("\n\n\n\n")

# print(df_airbnb.columns)

print("-------- df_airbnb --------")
print(df_airbnb.head(3))
print("-----------------------------")

print("\n\n\n\n")
print("-------------------------------- SPARK --------------------------------")
spark = SparkSession.builder.appName("Spark").getOrCreate()
df = spark.createDataFrame(df_airbnb)

df=df.withColumnRenamed("accomodation:address", "address")
df=df.withColumnRenamed("accomodation:bathrooms", "bathrooms")
df=df.withColumnRenamed("accomodation:bedrooms", "bedrooms")
df=df.withColumnRenamed("accomodation:beds", "beds")
df=df.withColumnRenamed("accomodation:cancelPolicy", "cancelPolicy")
df=df.withColumnRenamed("accomodation:city", "city")
df=df.withColumnRenamed("accomodation:deeplink", "deeplink")
df=df.withColumnRenamed("accomodation:lat", "lat")
df=df.withColumnRenamed("accomodation:lng", "lng")
df=df.withColumnRenamed("accomodation:name", "name")
df=df.withColumnRenamed("accomodation:persons", "persons")
df=df.withColumnRenamed("accomodation:position", "position")
df=df.withColumnRenamed("accomodation:type", "type")
df=df.withColumnRenamed("accomodation:url", "url")
df=df.withColumnRenamed("accomodation:userId", "userId")
df=df.withColumnRenamed("metadata:amenityIds", "amenityIds")
df=df.withColumnRenamed("metadata:hostThumbnail", "hostThumbnail")
df=df.withColumnRenamed("metadata:images", "images")
df=df.withColumnRenamed("metadata:isSuperhost", "isSuperhost")
df=df.withColumnRenamed("metadata:previewAmenities", "previewAmenities")
df=df.withColumnRenamed("metadata:rareFind", "rareFind")
df=df.withColumnRenamed("metadata:rating", "rating")
df=df.withColumnRenamed("metadata:reviewsCount", "reviewsCount")
df=df.withColumnRenamed("money:price_currency", "price_currency")
df=df.withColumnRenamed("money:price_priceItems", "price_priceItems")
df=df.withColumnRenamed("money:price_rate", "price_rate")
df=df.withColumnRenamed("money:price_total", "price_total")

print("-------- df --------")
df.show()
print("-----------------------------")

df_pandas = df.toPandas()
print(df_pandas.columns)
print("-----------------------------")

###################### histogramy po krajach ######################
df_selected = df_pandas[['address', 'city', 'price_rate', 'price_total']]
df_selected['price_total'] = df_selected['price_total'].astype(int)
df_selected['country'] = df_selected['address'].str.split(',').str[-1].str.strip()
print(df_selected.head(10))
print("-----------------------------")

# print(list(set(df_selected['city'])))
# print(list(set(df_selected['country'])))

df_selected.hist(column='price_total', by='country', bins=20, figsize=(12, 8), grid=False)
plt.xticks(np.arange(min(df_selected['price_total']), max(df_selected['price_total'])+1, 100.0), rotation='vertical')
plt.xlabel('Total price')
plt.ylabel('Frequency')
plt.suptitle('Histogram of total prices', x=0.5, y=1.02, fontsize=16)
plt.show()



####################################################################################

df_selected = df_pandas[['address', 'bedrooms', 'beds', 'rating', 'price_total']]
df_selected['price_total'] = df_selected['price_total'].astype(int)
df_selected['rating'] = df_selected['rating'].astype(float)
df_selected['country'] = df_selected['address'].str.split(',').str[-1].str.strip()
print(df_selected.head(10))
print("-----------------------------")

unique_countries = df_selected['country'].unique()
color_map = {country: plt.cm.viridis(i / (len(unique_countries))) for i, country in enumerate(unique_countries)}


###################### wykres punktowy: skumulowany ######################
plt.figure(figsize=(10, 6))

for country in unique_countries:
    country_data = df_selected[df_selected['country'] == country]
    plt.scatter(x=country_data['price_total'], y=country_data['rating'], c=[color_map[country]] * len(country_data), label=country)

plt.legend(title='Country')
plt.xlabel('Total price')
plt.ylabel('Rating')
plt.title('Comparison of Accommodation Prices to Ratings')
plt.show()


##################### wykres punktowy: poszczególne kraje ######################
fig, axes = plt.subplots(nrows=2, ncols=5, figsize=(15, 6))

for i, country in enumerate(unique_countries):
    country_data = df_selected[df_selected['country'] == country]
    row = i // 5
    col = i % 5
    axes[row, col].scatter(x=country_data['price_total'], y=country_data['rating'], c=[color_map[country]] * len(country_data), label=country)
    axes[row, col].set_title(country)
    axes[row, col].set_xlim(100, 1000)
    axes[row, col].set_ylim(3.5, 5.10)
    axes[row, col].set_xlabel('Total price')
    axes[row, col].set_ylabel('Rating')

plt.suptitle('Comparison of Accommodation Prices to Ratings Across Countries')
plt.tight_layout(rect=[0, 0, 0.95, 0.95])
plt.show()


print("\n\noki :3\n\n")
























































