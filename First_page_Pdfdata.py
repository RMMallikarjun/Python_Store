import os
import fitz
import psycopg2
import re


def extract_ctri_number_from_text(text):
    ctri_pattern = r"CTRI/\d{4}/\d{2}/\d{6}"
    match = re.search(ctri_pattern, text)
    if match:
        ctri_number = match.group()
        ctri_number = ctri_number.replace("/", "-")
        return ctri_number
    else:
        return None
        
def extract_last_updated_on(text):
    patern=r"Last\sModified\sO.\n(\d\d.\d\d.\d\d\d\d)"
    last_updated_on=re.search(patern,page_text,re.DOTALL) 
    if last_updated_on:       
        d=last_updated_on.group(1)           
        return d  
    else:
        return None
    
def extract_type_of_trial_from_text(text):
    type_of_trial_patterns =r'Type\sof\sTrial\n(.*)\nType\sof\sStudy'
    text1=re.search(type_of_trial_patterns,page_text,re.DOTALL)
    if text1:
        d8=text1.group(1)
        return d8   
    else:
        return None

def extract_type_of_study_from_text(text):
    type_of_trial_patterns =r'Type\sof\sStudy\n(.*)\nStudy\sDesign'
    #type_of_trial_patterns =r'Not\sIncluding\sYOGA.................'
    text1=re.search(type_of_trial_patterns,page_text,re.DOTALL)
    if text1:
        d8=text1.group(1)
        return d8   
    else:
        return None
        
def extract_Public_Title_from_text(text):
    type_of_trial_patterns =r'Public\sTitle\sof\sStudy\n(.*)\nScientific\sTitle\sof'
    #type_of_trial_patterns =r'Not\sIncluding\sYOGA.................'
    text1=re.search(type_of_trial_patterns,page_text,re.DOTALL)
    if text1:
        d8=text1.group(1)
        return d8   
    else:
        return None
    
if __name__ == "__main__":
    host = "10.140.189.165"
    database = "Internal"
    username = "postgres"
    password = "techsol"
    folder_path = r"E:\Clinical_data6"
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
                CREATE TABLE IF NOT EXISTS CTRI_PDF_PAGE1_DATA1 (
                    id SERIAL PRIMARY KEY,
                    ctri_number VARCHAR,
					last_updated_on varchar,
                    Type_of_Trail varchar,
                    Type_of_Study varchar,
                    title_of_study varchar,  
                    file_name varchar
                );
            """
            cursor.execute(sql_query)
            connection.commit()
            for pdf_file in pdf_files:
                pdf_path = os.path.join(folder_path, pdf_file)
                
                with fitz.open(pdf_path) as pdf:
                    page = pdf.load_page(0)
                    page_text = page.get_text()
                    
                    ctri_number=extract_ctri_number_from_text(page_text)
                    last_updated_on=extract_last_updated_on(page_text)
                    Type_of_Trail=extract_type_of_trial_from_text(page_text)
                    Type_of_Study=extract_type_of_study_from_text(page_text)
                    title_of_study=extract_Public_Title_from_text(page_text)
                sql_insert_query = "INSERT INTO CTRI_PDF_PAGE1_DATA1 (ctri_number,last_updated_on,Type_of_Trail,Type_of_Study,title_of_study,file_name) VALUES (%s,%s,%s,%s,%s,%s);"
                cursor.execute(sql_insert_query, (ctri_number,last_updated_on,Type_of_Trail,Type_of_Study,title_of_study,pdf_file))
                connection.commit()
            else:
                    print(f"No CTRI number will be retrieved for {pdf_file} as Type of Trial is not BA/BE.")

            cursor.close()
            connection.close()
            print("Data inserted successfully into PostgreSQL!")
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL or inserting data:", error)
                
            
