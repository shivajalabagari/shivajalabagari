# Spark Data Processing Assignment

## 1. Downloaded data from  API
- **Task**: The `get_data_by_brand` function fetches data  and  it refers to local JSON files (`{brand}-places.json
- **CODE**:
    

    def get_data_by_brand(brand: str, logger: logging.Logger = LOGGER) -> DataFrame:
        # Fetch data from local JSON file based on brand
    '''

## 2. Created Logger Object
- **Task**: Set up logging to a file named `assignment.log`. i created a logger object configured it to log messages at the INFO level.
- **Implementation**:
    CODE
    LOGGER = logging.getLogger()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    ``

## 3. Implement `get_data_by_brand` Function
- **Task**: Load data for brand, handle errors, and ensure the data includes a `brand` column.
- **Implementation**:
    ``CODE
    def get_data_by_brand(brand: str, logger: logging.Logger = LOGGER) -> DataFrame:
        # For errors
        df = spark.read.json(file_path)
        df = df.withColumn("brand", lit(brand))
    ``

## 4. Write in a single DataFrame
- **Task**: Combine DataFrames from all brands into one.
- **Implementation**:
    ``CODE
    def union_all(dfs: list) -> DataFrame:
        return reduce(lambda df1, df2: df1.unionByName(df2), dfs)
    ``
## 5. Drop `placeSearchOpeningHours` Column
- **Task**: Remove the `placeSearchOpeningHours` column if it exists.
- **Implementation**:
    ``CODE
    if "placeSearchOpeningHours" in df.columns:
        df = df.drop("placeSearchOpeningHours")
    ``

## 6. kept `sellingPartners` as Array
- **Task**: Ensure `sellingPartners` is treated as an array of strings.
- **Implementation**:
    ``CODE
    if "sellingPartners" in df.columns:
        df = df.withColumn("sellingPartners", col("sellingPartners").cast(ArrayType(StringType())))
        df = df.withColumn("sellingPartners", when(col("sellingPartners").isNull(), lit([]))
                                            .otherwise(col("sellingPartners")))
    ``

## 7. Extract `postal_code` from Address
- **Task**: Extract the postal code from the address column.
- **Implementation**:
    ``CODE
    postal_code_udf = udf(lambda address: address.split()[-1] if address else None, StringType())
    df = df.withColumn("postal_code", postal_code_udf(col("address")))
    ``

## 8. Create `province` Column from `postal_code`
- **Task**: Derive a `province` column based on the `postal_code`.
- **Implementation**:
    ``CODE
    df = df.withColumn("province", when(col("postal_code").between("1000", "1299"), "Brussel")
                                    .when(col("postal_code").between("1300", "1499"), "Waals-Brabant")
                                    .when(col("postal_code").between("1500", "1999"), "Vlaams-Brabant")
                                    .when(col("postal_code").between("2000", "2999"), "Antwerpen")
                                    .when(col("postal_code").between("3000", "3499"), "Vlaams-Brabant")
                                    .when(col("postal_code").between("3500", "3999"), "Limburg")
                                    .when(col("postal_code").between("4000", "4999"), "Luik")
                                    .when(col("postal_code").between("5000", "5999"), "Namen")
                                    .when(col("postal_code").between("6000", "6599"), "Henegouwen")
                                    .when(col("postal_code").between("7000", "7999"), "Henegouwen")
                                    .when(col("postal_code").between("6600", "6999"), "Luxemburg")
                                    .when(col("postal_code").between("8000", "8999"), "West-Vlaanderen")
                                    .when(col("postal_code").between("9000", "9999"), "Oost-Vlaanderen")
                                    .otherwise("Unknown"))
    ``

## 9. Transform `geoCoordinates` into `latitude ` and `longitude ` Columns
- **Task**: Extract latitude and longitude from `geoCoordinates` and create `lat` and `lon` columns.
- **Implementation**:
    ``CODE
    if "geoCoordinates" in df.columns:
        df = df.withColumn("lat", col("geoCoordinates.latitude"))
        df = df.withColumn("lon", col("geoCoordinates.longitude"))
    ``

## 10. `handoverServices` to Encode
- **Task**: Transform the `handoverServices` column into encoded format.
- **Implementation**:
    ``CODE
    if "handoverServices" in df.columns:
        df = df.withColumn("handoverServices", col("handoverServices").cast("string"))
        df = df.withColumn("handoverServices", when(col("handoverServices").isNull(), lit("None"))
                                            .otherwise(col("handoverServices")))
    ``

## 11. Anonymize GDPR Sensitive Data
- **Task**: Anonymize `houseNumber` and `streetName` columns.
- **Implementation**:
    ``CODE
    df = df.withColumn("houseNumber", lit("ANONYMIZED"))
    df = df.withColumn("streetName", lit("ANONYMIZED"))
    ``

## 12. Save Result as Parquet File
- **Task**: Save the processed DF as a parquet file, partitioned by `postal_code`.
- **Implementation**:
    ``CODE
    df.write.partitionBy("postal_code").mode("overwrite").parquet(path)
    ``

## Summary
- **Logger**: Configured to log messages to a file `assignment.log`.
- **Data Handling**: Combined data from multiple brands into a single DataFrame.
- **Transformations**: used  dropping columns, extracting postal codes, and handling GDPR data.
- **Output**: Saved the final DataFrame in parquet format.
