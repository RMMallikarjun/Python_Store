import os
import fitz
import psycopg2
import re

def extract_data_from_pdf(pdf_path):
    text = ""
    with fitz.open(pdf_path) as pdf:
        for page_number in range(pdf.page_count):
            page = pdf.load_page(page_number)
            text += page.get_text()

    return text

def extract_primary_sponsor_details(text):
    primary_sponsor_pattern = r"Primary Sponsor\s+Details\s*\nName\s+([^(\n]+)\s+Address\s+([^T]+(?:(?!Details of Secondary Sponsor).)*)\s+Type of Sponsor\s+([^(\n]+)"
    match = re.search(primary_sponsor_pattern, text, re.DOTALL)
    if match:
        sponsor_name = match.group(1).strip()
        sponsor_address = match.group(2).strip()
        sponsor_type = match.group(3).strip()
        return sponsor_name, sponsor_address, sponsor_type
    else:
        return None, None, None
def extract_Secondary_sponsor_details(text):
    #primary_sponsor_pattern = r"Primary Sponsor\s+Details\s*\nName\s+([^(\n]+)\s+Address\s+([^T]+(?:(?!Details of Secondary Sponsor).)*)\s+Type of Sponsor\s+([^(\n]+)"
   # primary_sponsor_pattern=r'Primary\sSponsor\sDetails\nName\n(.*)\n+Address\n(.*)\n(.*)\nType\sof\sSponsor\n(.*)\nDetails\sof\sSecondary\sSponsor'
    #Details\sof\sSecondary\nSponsor\nName\nAddress\n(*)Countries\sof
    Secondary_sponsor_pattern=r'Details\sof\sSecondary\nSponsor\nName\nAddress\n(.*)Countries\sof'
    match = re.search(Secondary_sponsor_pattern, text, re.DOTALL)
    if match:
        S_sponsor_name = match.group(1)
       # sponsor_address = match.group(2).strip()
        #sponsor_type = match.group(3).strip()
        
        return S_sponsor_name
    else:
        return None

if __name__ == "__main__":
    host = "10.140.189.165"
    database = "Internal"
    username = "postgres"
    password = "techsol"

    folder_path = r"E:\New folder"

    pdf_files = [file for file in os.listdir(folder_path) if file.endswith(".pdf")]

    if len(pdf_files) == 0:
        print("No PDF files found in the folder.")
    else:
        try:
            connection = psycopg2.connect(
                host=host,
                database=database,
                user=username,
                password=password
            )
            cursor = connection.cursor()

            sql_query = """
                CREATE TABLE IF NOT EXISTS ctri_pdf_page1_Sponsordata4 (
                    id SERIAL PRIMARY KEY,
                    pdf_file_name VARCHAR,
                    sponsor_name VARCHAR,
                    sponsor_address TEXT,
                    sponsor_type VARCHAR,
                    Secondary_Sponsor varchar
                );
            """
            cursor.execute(sql_query)
            connection.commit()

            for pdf_file in pdf_files:
                pdf_path = os.path.join(folder_path, pdf_file)
                pdf_file_name = os.path.basename(pdf_path)
                with fitz.open(pdf_path) as pdf:
                    for page_number in range(pdf.page_count):
                        page = pdf.load_page(page_number)
                        page_text = page.get_text()

                        sponsor_name, sponsor_address, sponsor_type = extract_primary_sponsor_details(page_text)
                        Secondary_Sponsor = extract_Secondary_sponsor_details(page_text)

                        if sponsor_name and sponsor_address and sponsor_type:
                            #print(f"Processing {pdf_file}")
                           # print(f"Processing {pdf_file}")
                           # print(f"Sponsor Name: {sponsor_name}")
                           # print(f"Sponsor Address: {sponsor_address}")
                           # print(f"Sponsor Type: {sponsor_type}")

                            # Store data in the database
                            sql_insert_query = "INSERT INTO ctri_pdf_page1_Sponsordata4 (pdf_file_name,sponsor_name, sponsor_address, sponsor_type,Secondary_Sponsor) VALUES (%s,%s, %s, %s,%s);"
                            cursor.execute(sql_insert_query, (pdf_file_name, sponsor_name, sponsor_address, sponsor_type,Secondary_Sponsor))
                            connection.commit()
                            break  # Break after finding the primary sponsor details on the first page

            cursor.close()
            connection.close()
            print("Data inserted successfully into PostgreSQL!")
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL or inserting data:", error)
