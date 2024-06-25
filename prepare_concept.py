from connector import get_connection
import pandas as pd

Matcho_concepts = pd.read_excel("Matcho_concepts.xlsx")

def concepts_in_enclave(Matcho_concepts, concept):
    df = Matcho_concepts[["concept_id", "category"]]
    result = pd.merge(df, concept, on="concept_id", how="inner")
    return result

def filter_non_standard(concepts_in_enclave):
    df = concepts_in_enclave[concepts_in_enclave["standard_concept"].isnull()]
    return df

def filter_standard(concepts_in_enclave):
    df = concepts_in_enclave[concepts_in_enclave["standard_concept"] == "S"]
    return df

def map_to_standard(filter_non_standard, concept_relationship):
    relationships = concept_relationship.drop(columns=["valid_start_date", "valid_end_date", "invalid_reason"])
    mapped_concepts = pd.merge(filter_non_standard, relationships, left_on="concept_id", right_on="concept_id_1", how="inner")
    mapped_concepts = mapped_concepts[mapped_concepts["relationship_id"] == "Maps to"]
    return mapped_concepts

def get_standard_columns(map_to_standard, concept):
    updated_concepts = map_to_standard[["concept_id_2", "category"]]
    updated_concepts = updated_concepts.rename(columns={"concept_id_2": "concept_id"})
    updated_final = pd.merge(updated_concepts, concept, on="concept_id", how="inner")
    return updated_final

def combine_tables(get_standard_columns, filter_standard):
    union_df = pd.concat([filter_standard, get_standard_columns], ignore_index=True)
    return union_df

def main(conn):
    # Read concept and concept_relationship tables from SQL Server
    print("Reading data from SQL Server, this might take a while...")
    concept = pd.read_sql("SELECT * FROM vocab.concept", conn)
    concept_relationship = pd.read_sql("SELECT * FROM vocab.concept_relationship", conn)
    
    print("Processing data...")
    # Extract and process data
    concepts_in_enclave_df = concepts_in_enclave(Matcho_concepts, concept)
    filter_non_standard_df = filter_non_standard(concepts_in_enclave_df)
    filter_standard_df = filter_standard(concepts_in_enclave_df)
    map_to_standard_df = map_to_standard(filter_non_standard_df, concept_relationship)
    get_standard_columns_df = get_standard_columns(map_to_standard_df, concept)
    Matcho_concepts_df = combine_tables(get_standard_columns_df, filter_standard_df)

    return Matcho_concepts_df

if __name__ == "__main__":
    conn = get_connection()

    Matcho_concepts = pd.read_excel("Matcho_concepts.xlsx")

    matcho_df = main(conn)

    matcho_df.to_csv("data\macho_concept_mapped.csv", index=False)
    print("Data saved to data\macho_concept_mapped.csv")