from pyspark.sql import SparkSession
from pyspark.sql.functions import when, split, col

# Sukuriam Spark sesiją
spark = SparkSession.builder.appName("PedagoguKvalifikacija").getOrCreate()

# Įkeliam failą
df = spark.read.csv('Pedagogu kvalifikacija-12-lt-lt.csv', header=True, sep='\t')

# Jei tekstiniuose laukuose reikšmė yra null, ją pakeičiam tekstu "Nenurodyta".
df = df.na.fill({'Pd Institucijos savivaldybė': 'Nenurodyta'})

print(df.columns)

# Stulpelius pervadinam į snake_case stilių
for col_name in df.columns:
    df = df.withColumnRenamed(col_name, col_name.lower().replace(' ', '_'))

# Nustatom tinkamus stulpelių duomenų tipus ir atliekam transformacijas
split_col = split(df['pd_mokslo_metai'], '-')
df = df.withColumn('start_year', split_col.getItem(0).cast("int"))
df = df.withColumn('end_year', split_col.getItem(1).cast("int"))

# Paruošiam lentelę, rodančią mokytojų kiekį kiekvienoje savivaldybėje
mokytojai_df = df.filter(df.pd_pareigų_grupė == 'Mokytojai')
mokytoju_kiekis = mokytojai_df.groupBy('pd_institucijos_savivaldybė').count().withColumnRenamed('count', 'mokytoju_kiekis')
visos_savivaldybes = mokytoju_kiekis.agg({"mokytoju_kiekis": "sum"}).collect()[0][0]
mokytoju_kiekis = mokytoju_kiekis.union(spark.createDataFrame([("Viso", visos_savivaldybes)], ["pd_institucijos_savivaldybė", "mokytoju_kiekis"]))


# Išsaugom rezultatą Excel faile
mokytoju_kiekis.toPandas().to_excel('rezultatas.xlsx', index=False)

# Uždarom Spark sesiją
spark.stop()