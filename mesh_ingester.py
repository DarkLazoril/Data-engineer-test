import pandas as pd # type: ignore
import numpy as np # type: ignore
import sys
from sqlalchemy import create_engine, Column, String, Float, Boolean, Integer, Table, MetaData # type: ignore
from sqlalchemy.orm import declarative_base, sessionmaker # type: ignore

Base = declarative_base()

class Mesh(Base):
    __tablename__ = 'mesh'
    
    codename = Column(String, primary_key=True)
    trame = Column(String, nullable=False)
    mass_surf = Column(Float, nullable=False)
    is_compat_interior_wall = Column(Boolean, nullable=False)
    mesh_height = Column(Float, nullable=False)
    mesh_width = Column(Float, nullable=False)
    roll_pallet = Column(Integer)
    color_names = Column(String)  

def convert_and_validate_colors(color_names_str, possible_colors):
    colors_list = color_names_str.split(',')
    if all(color.strip() in possible_colors for color in colors_list):
        return colors_list
    else:
        raise ValueError(f"Invalid color names in the list. Got {color_names_str}")
    
def validate_trame(value, valid_values):
    if value not in valid_values:
        raise ValueError(f"Value {value} for trame is not valid.")
    
def validate_mass_surf(value):
    try:
        mass_surf_value = float(value)
        if mass_surf_value < 0:
            raise ValueError(f"mass_surf must be non-negative. Got {mass_surf_value}")
        return mass_surf_value
    except ValueError:
        raise ValueError(f"mass_surf must be a float. Got {value}")
    
def validate_bool(value):
    true_values = ["vrai", "true", 1, "1"]
    false_values = ["faux", "false", 0, "0"]
    if  str(value).lower() in true_values:
        return True
    elif str(value).lower() in false_values:
        return False
    else:
        raise ValueError(f"Expected boolean value. Got {value}")

def validate_positive_float(value, field_name):
    try:
        float_value = float(value)
        if float_value < 0:
            raise ValueError(f"{field_name} must be non-negative. Got {float_value}")
        return float_value
    except ValueError:
        raise ValueError(f"{field_name} must be a float. Got {value}")

def validate_optional_int(value):
    if pd.isna(value) or value == '':
        return None
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"Expected integer value. Got {value}")

def ingest_and_validate_csv(csv_file_path):
    df = pd.read_csv(csv_file_path, delimiter=';', na_values=["", "NaN", " "], keep_default_na=True)
    # print(df.dtypes)
    # print(df)
    df['exclude'] = False
    error_messages = []
    duplicated_status = df['codename'].duplicated(keep=False)
    required_fields = ['codename', 'trame', 'mass_surf', 'is_compat_interior_wall', 'mesh_height', 'mesh_width','color_names']
    valid_trames = ["T2 Ra1 M2 E2", "T2 Ra1 M4 E2", "T2 Ra1 M4 E3"]
    valid_colors = ["white", "yellow", "green", "purple", "red", "blue", "orange", "magenta", "dark", "grey", "cyan"]
    
    for i, row in df.iterrows():
        row_errors = [] 
        
        for column in required_fields:
            if pd.isnull(row[column]):
                row_errors.append(f"Empty value at line {i + 1} for '{column}'.")
        
        if duplicated_status.iloc[i]:
            row_errors.append(f"Duplicate codename found at line {i + 1}.")
        
        try:
            validate_trame(row['trame'], valid_trames)
        except ValueError as e:
            row_errors.append(f"Error at line {i+1}: {e}")
        
        try:
            df.at[i, 'mass_surf'] = validate_mass_surf(row['mass_surf'])
        except ValueError as e:
            row_errors.append(f"Error at line {i+1}: {e}")
        
        try:
            df.at[i, 'is_compat_interior_wall'] = validate_bool(row['is_compat_interior_wall'])
        except ValueError as e:
            row_errors.append(f"Error at line {i+1}: {e}")
        
        try:
            df.at[i, 'mesh_height'] = validate_positive_float(row['mesh_height'], 'mesh_height')
            df.at[i, 'mesh_width'] = validate_positive_float(row['mesh_width'], 'mesh_width')
        except ValueError as e:
            row_errors.append(f"Error at line {i+1}: {e}")
        
        try:
            df.at[i, 'roll_pallet'] = validate_optional_int(row['roll_pallet'])
        except ValueError as e:
            row_errors.append(f"Error at line {i+1}: {e}")
        
        try:
            df.at[i, 'color_names'] = convert_and_validate_colors(row['color_names'], valid_colors)
        except ValueError as e:
            row_errors.append(f"Error at line {i+1}: {e}")

        if row_errors:

            error_messages.extend(row_errors)
            df.at[i, 'exclude'] = True
    
    df = df[~df['exclude']]
    df.drop(columns='exclude', inplace=True)
    
    return df, error_messages

# Database connection and table creation (using SQLite)
def setup_database():
    engine = create_engine('sqlite:///mesh.db')
    Base.metadata.create_all(engine)
    return engine

# Inserting validated data into the database
def insert_data_to_db(df, engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    
    for index, row in df.iterrows():
        mesh = Mesh(
            codename=row['codename'],
            trame=row['trame'],
            mass_surf=row['mass_surf'],
            is_compat_interior_wall=row['is_compat_interior_wall'],
            mesh_height=row['mesh_height'],
            mesh_width=row['mesh_width'],
            roll_pallet=row.get('roll_pallet'),
            color_names=','.join(row['color_names'])
        )
        session.add(mesh)
    
    session.commit()

def main(csv_file_path):
    engine = setup_database()
    df, error_messages = ingest_and_validate_csv(csv_file_path)
    
    if error_messages:
        print("Errors encountered:")
        for message in error_messages:
            print(message)
    else:
        insert_data_to_db(df, engine)
        print("Data ingested and validated successfully.")

# csv_file_path = sys.argv[1]
csv_file_path = input("Please enter the CSV file path: ")
main(csv_file_path)
