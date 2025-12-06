# elgo_data_pipeline
ข้อมูล Business Sales Dataset ที่ได้มาจากคนรู้จักนั้น มีทั้งหมด 13 tables ซึ่งมาจาก database ที่ยังไม่นิ่ง ไม่ได้ผ่านการออกแบบมาดี มีความซ้ำซ้อนอยู่มาก และรับข้อมูลมาจากหลายช่องทางการขาย/platform รวมกัน หลังจากดูข้อมูลคร่าว ๆ ก็ได้ไปคุยกับเจ้าของข้อมูลเพื่อทำความเข้าใจมากขึ้น
## Data integration
เนื่องจากมีความซ้ำซ้อนอยู่มากจึงต้อง normalize ข้อมูล โดยเริ่มจากการไปช่วยออกแบบ ER diagrams ให้ใหม่ก่อน เพื่อวางแผนหน้าตาโครงสร้างข้อมูลใหม่ที่เราควรจะแปลงไปด้วย
หลังจากออกแบบใหม่ก็จะเลือกตัดส่วนที่ดูไม่เกี่ยวข้องกับการขายออกไปบางส่วน เช่น table inventory_input ที่เกี่ยวข้องกับการจัดการ stock สินค้า เพื่อนำมาใช้กับโปรเจคนี้ 
และข้อมูลบางส่วนก็จะไม่ได้ normalize จนเป็นหน่วยเล็กที่สุด ออกแบบจนได้ ER diagram เป้าหมายออกมาดังภาพ (บางคอลัมน์ที่ยังไม่มีการเก็บข้อมูลเพิ่มเข้ามาก็จะข้ามไปก่อน)

![Elgo New Database](https://github.com/user-attachments/assets/31200394-75e1-4885-a04d-ff861af3ec3d)

### Installation
Data integration จะฝึกใช้ polars เป็นหลัก
>ไม่ได้อัพโหลด CSV address, canceled_order, customer_code, sale_order, waybill ให้มาเพราะมีข้อมูลส่วนตัวอยู่ !

กรณีลองใช้บน [Google Colab](https://colab.research.google.com/drive/1_eGQVK0TWu8GBq6w9AqMe8WNffU6Lh7z?usp=sharing) หรือเปิดไฟล์ .ipynb<br>
สามารถนำเข้า csv จากโฟลเดอร์ใน git ..\example_data\renamed_source_database ไปวางไว้นอกโฟลเดอร์ sample_data ของ Google Colab ก่อนเริ่มรันได้เลย

กรณีจะรันไฟล์ python<br>
Run these commands in Command Prompt or PowerShell

    pip install polars
    python scripts\preprocessing.py

### Data Integration Plan Steps
1. Extract and rename tables
2. Transform - Schema/Value Integration
    - Rename columns
    - Change format
    - รวม data
    - แยก data
    - สร้าง fake values
    - สรุปผลข้อมูล
    - Check Value
3. Load - Insert data into MySQL Database
    - สร้าง ENVIRONMENT_VARIABLE ใช้บน local
    - ใช้ Sqlalchemy เชื่อมต่อไปที่ MySQL
    - สร้าง Schema (database)
    - Insert ข้อมูลลงใน tables
4. Query ข้อมูลใน table และเซฟ output
    - Query ข้อมูล
    - Output ไฟล์เป็น parquet

## Data Cleansing
### Installation
Data Cleansing จะฝึกใช้ PySpark เป็นหลัก<br>
Run these commands in Command Prompt or PowerShell

    !apt-get update
    !apt-get install openjdk-8-jdk-headless -qq > /dev/null
    !wget -q https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
    !tar xzvf spark-3.5.1-bin-hadoop3.tgz
    !pip install -q findspark

    import os
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ["SPARK_HOME"] = "/content/spark-3.5.1-bin-hadoop3"

    !pip install pyspark==3.5.1
