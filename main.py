import pandas as pd


def prepare_dc(dc_path: str) -> pd.DataFrame:
    """
    Returns cleaned up dc dataframe.
    """
    df = pd.read_excel(dc_path)

    df = df[df.columns[2:6]]
    df[df.columns[0]] = df[df.columns[0]].apply(lambda s: int(s[2:]))
    dfdc_col_rename_mapping = {
        df.columns.values[0]: "ad_id",
        df.columns.values[1]: "impressions",
        df.columns.values[2]: "clicks",
        df.columns.values[3]: "spent",
    }
    df.rename(columns=dfdc_col_rename_mapping, inplace=True)

    return df


def prepare_ad(ad_path: str) -> pd.DataFrame:
    """
    Returns cleaned up ad dataframe.
    """
    df = pd.read_csv(ad_path, encoding="utf-16", skiprows=2, sep="\t")

    df = df[df.columns[-4:]]
    df.replace(pd.NA, "", inplace=True)
    dfad_col_rename_mapping = {
        df.columns.values[0]: "ad_id",
        df.columns.values[1]: "title_1",
        df.columns.values[2]: "title_2",
        df.columns.values[3]: "text",
    }
    df.rename(columns=dfad_col_rename_mapping, inplace=True)

    return df


def prepare_rc(rc_path: str) -> pd.DataFrame:
    """
    Returns cleaned up rc dataframe.
    """
    df = pd.read_excel(rc_path)

    df = df.iloc[:, [-2, 8, 9, 10, 11]]
    df = df[df["utm_content.1"].notna()]
    df["utm_content.1"] = df["utm_content.1"].apply(lambda s: int(s.split("|")[1][4:]))
    df.fillna(0, inplace=True)
    df.replace({"Да": 1, "Нет": 0}, inplace=True)
    dfrc_col_rename_mapping = {
        df.columns.values[0]: "ad_id",
        df.columns.values[1]: "kp",
        df.columns.values[2]: "budget_approval",
        df.columns.values[3]: "objection",
        df.columns.values[4]: "finished",
    }
    df.rename(columns=dfrc_col_rename_mapping, inplace=True)
    df = df.groupby("ad_id").count()

    return df


def get_report(dc_path: str, ad_path: str, rc_path: str):
    """
    Composes a report based on provided files.
    """
    dfdc = prepare_dc(dc_path)
    dfad = prepare_ad(ad_path)
    dfrc = prepare_rc(rc_path)

    report = (
        dfrc.join(dfdc.set_index("ad_id")).join(dfad.set_index("ad_id")).reset_index()
    )

    return report


if __name__ == "__main__":
    report = get_report(
        dc_path="./files/direct_cost.xlsx",
        ad_path="./files/ads_direct.csv .csv",
        rc_path="./files/report_CRM.xlsx",
    )
    print(report)
