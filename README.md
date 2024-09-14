# elgo_data_pipeline
ข้อมูล Business Sales Dataset ที่ได้มาจากคนรู้จักนั้น มีทั้งหมด 14 tables ซึ่งมาจาก database ที่ยังไม่นิ่ง ไม่ได้ผ่านการออกแบบมาดี มีความซ้ำซ้อนอยู่มาก และรับข้อมูลมาจากหลายช่องทางการขาย/platform รวมกัน หลังจากดูข้อมูลคร่าว ๆ ก็ได้ไปคุยกับเจ้าของข้อมูลเพื่อทำความเข้าใจมากขึ้น

## Data integration
เนื่องจากมีความซ้ำซ้อนอยู่มากจึงต้อง normalize ข้อมูล โดยเริ่มจากการไปช่วยออกแบบ ER diagrams ให้ใหม่ก่อน เพื่อวางแผนหน้าตาโครงสร้างข้อมูลใหม่ที่เราควรจะแปลงไปด้วย
หลังจากออกแบบใหม่ก็จะเลือกตัดส่วนที่ดูไม่เกี่ยวข้องกับการขายออกไปบางส่วน เช่น table inventory_input ที่เกี่ยวข้องกับการจัดการ stock สินค้า เพื่อนำมาใช้กับโปรเจคนี้ 
และข้อมูลบางส่วนก็จะไม่ได้ normalize จนเป็นหน่วยเล็กที่สุด ออกแบบจนได้ ER diagram เป้าหมายออกมาดังภาพ (บางคอลัมน์ที่ยังไม่มีการเก็บข้อมูลเพิ่มเข้ามาก็จะข้ามไปก่อน)

![Elgo New Database](https://github.com/user-attachments/assets/31200394-75e1-4885-a04d-ff861af3ec3d)

Data Integration Plan Steps
1. Extract and rename tables
2. Transform - Schema/Value Integration
    - Rename columns
    - Change format and add day
    - รวม data
    - แยก data
    - สร้าง fake values
    - สรุปผลข้อมูล
    - Check Value
3. Load - Insert data into MySQL Database
    - อ่าน Secrets ของ Colab (เก็บลง class Config)
    - ใช้ Sqlalchemy เชื่อมต่อไปที่ MySQL
    - สร้าง Schema (database)
    - Insert ข้อมูลลงใน tables
4. Query ข้อมูลใน table และเซฟ output
    - Query ข้อมูลด้วย Pandas
    - Output ไฟล์เป็น parquet
5. ทดลองอ่านไฟล์ parquet เพื่อตรวจสอบ

## How to run scripts
1. Run `pip install polars` in Command Prompt or PowerShell
2. Run `python3 ..\elgo_data_pipeline\scripts\preprocessing.py`
