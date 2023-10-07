from pyspark.sql import SparkSession
from pyspark.sql.functions import when, split, col, countDistinct

# Sukuriam Spark sesiją
spark = SparkSession.builder.appName("PedagoguKvalifikacija").getOrCreate()

# Įkeliam failą
df = spark.read.csv('Pedagogu kvalifikacija-12-lt-lt.csv', header=True, sep='\t')

# Jei tekstiniuose laukuose reikšmė yra null, ją pakeičiam tekstu "Nenurodyta".
df = df.na.fill({'Pd Institucijos savivaldybė': 'Nenurodyta'})

# Stulpelius pervadinam į snake_case stilių, pašalinant lietuviškus simbolius
trans_dict = {'ą': 'a', 'č': 'c', 'ę': 'e', 'ė': 'e', 'į': 'i', 'š': 's', 'ų': 'u', 'ū': 'u', 'ž': 'z'}
for col_name in df.columns:
    new_name = col_name.lower().replace(' ', '_')
    for original, replacement in trans_dict.items():
        new_name = new_name.replace(original, replacement)
    df = df.withColumnRenamed(col_name, new_name)

# Nustatom tinkamus stulpelių duomenų tipus ir atliekam transformacijas
split_col = split(df['pd_mokslo_metai'], '-')
df = df.withColumn('start_year', split_col.getItem(0).cast("int"))
df = df.withColumn('end_year', split_col.getItem(1).cast("int"))

# Patikriname, ar yra persidengimų tarp metų ()
years_distinct = df.select(countDistinct("start_year"), countDistinct("end_year")).collect()[0]
if years_distinct[0] != years_distinct[1]:
    print("Yra metų persidengimų!")
else:
    print("Nėra metų persidengimų.")

# Paruošiam lentelę, rodančią mokytojų kiekį kiekvienoje savivaldybėje
mokytojai_df = df.filter(df.pd_pareigu_grupe == 'Mokytojai')
mokytoju_kiekis = mokytojai_df.groupBy('pd_institucijos_savivaldybe').count().withColumnRenamed('count', 'mokytoju_kiekis')
visos_savivaldybes = mokytoju_kiekis.agg({"mokytoju_kiekis": "sum"}).collect()[0][0]
mokytoju_kiekis = mokytoju_kiekis.union(spark.createDataFrame([("Viso", visos_savivaldybes)], ["pd_institucijos_savivaldybe", "mokytoju_kiekis"]))

# Išsaugom rezultatą Excel faile
mokytoju_kiekis.toPandas().to_excel('rezultatas.xlsx', index=False)

# Uždarom Spark sesiją
spark.stop()
