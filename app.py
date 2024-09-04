from folium.plugins import MarkerCluster
from pyspark.sql import functions as F
from streamlit_folium import st_folium
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import streamlit as st
import pandas as pd
import requests
import folium


st.title('''DublinBikes app''')

@st.cache_data
def d_bikes(nrows):
    url = 'https://api.jcdecaux.com/vls/v1/stations'
    api_key = '5c07ed433d8e7357f3fc3abb861d9eb6c79c8163'

    params = {
        'contract': 'Dublin',
        'apiKey': api_key
    }

    response = requests.get(url, params=params)


    if response.status_code == 200:
        data = response.json()

    spark = SparkSession.builder.appName("DublinBike").getOrCreate()

    schema = StructType([
        StructField("number", StringType(), True),
        StructField("contract_name", StringType(), True),
        StructField("name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("position", MapType(StringType(), DoubleType(), True), True),
        StructField("banking", BooleanType(), True),
        StructField("bike_stands", StringType(), True),
        StructField("available_bikes", LongType(), True),
        StructField("status", StringType(), True),
        StructField("last_update", LongType(), True),
    ])

    df = spark.createDataFrame(data, schema)

    df = df.select(
        col("number"),
        col("contract_name"),
        col("name"),
        col("address"),
        col("position.lat").alias("latitude"),
        col("position.lng").alias("longitude"),
        col("banking"),
        col("bike_stands"),
        col("available_bikes"),
        col("status"),
        col("last_update")
    )

    df = df.withColumn("last_update", col("last_update") / 1000)

    df = df.withColumn("last_update", from_unixtime(col("last_update")))

    df_pandas = df.toPandas()
    return df_pandas

df_pandas = d_bikes(1000)

if st.checkbox('Show raw data'):
    st.write(df_pandas)

st.subheader('Number of Bikes Available per Station')
fig, ax = plt.subplots(figsize=(20, 10))
df_pandas.plot(kind='bar', x='name', y='available_bikes', ax=ax, color='skyblue')
plt.xticks(rotation=90, ha='right', fontsize=7)
plt.xlabel('Station')
plt.ylabel('Available Bikes')
plt.title('Available Bikes by Station')

plt.yticks(np.arange(0, 45, 1))

plt.subplots_adjust(bottom=0.01)

st.pyplot(fig)

m = folium.Map(location=[53.349805, -6.26031], zoom_start=14)


marker_cluster = MarkerCluster().add_to(m)

for _, row in df_pandas.iterrows():
    folium.Marker(
        location=[row['latitude'], row['longitude']],
        popup=f"{row['name']}: {row['available_bikes']} available bikes",
        icon=folium.Icon(color='blue', icon='info-sign')
    ).add_to(marker_cluster)


st.subheader('Interactive Map with Bicycle Stations.')
st.markdown("The map below shows the location of the bike stations and the quantity available at each one.")
st_folium(m, width=2000)