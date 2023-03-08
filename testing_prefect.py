from prefect import flow, task
import requests
import pandas as pd
import openpyxl
import os


@task(description="Fetch data")
def call_url(url):
    resp = requests.get(url).content
    df_meds = pd.read_excel(
        resp, engine="openpyxl", skiprows=[0], header=[1]
    )
    return df_meds


@flow(name="Get the meds", description="Init flow")
def get_meds(url):
    df_meds = call_url(url)
    df_meds_clean = fix_schema(df_meds)
    return df_meds_clean


@task(name="transform med dataframe", description="Rename english names of header")
def fix_schema(df):
    curated = df.rename(
        columns={
            "Name of Product" : "name",
            "Active Ingredient" : "active_ingredient",
            "Pharmaceutical Form" : "pharmaceutical_form",
            "Strength" : "strength",
            "ATC Code" : "atc_code",
            "Legal Status" : "legal_status",
            "MAH" : "agency",
            "Marketing authorisation number" : "marketing_authorisation_number",
            "Other Information" : "other_information",
            "Marketed" : "marketed",
            "MA Issued" : "marketed_issued"
        }
    )
    return curated


@flow(name="Write to csv based on local folder")
def write_df_to_csv(df) -> None:
    path = f"{os.getcwd()}/medicine.csv"
    df.to_csv(path)

## Fetch this set of medicine from agency of medicine.
def main():
    url = "https://www.lyfjastofnun.is/wp-content/uploads/2023/03/lyf-med-markadsleyfi-0323.xlsx"
    clean_meds = get_meds(url)
    write_df_to_csv(clean_meds)


if __name__ == "__main__":
    main()
