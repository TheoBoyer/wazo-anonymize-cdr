import os
import warnings
import pandas as pd
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

columns = [
    'id', # DONE
    'start', # DONE
    'end', # DONE
    'answered', # DONE
    'answer', # DONE
    'duration', # DONE
    'call_direction', # DONE
    'destination_extension', # DONE
    'destination_internal_context', # DONE
    'destination_internal_extension', # DONE
    'destination_line_id', # DONE
    'destination_name', # DONE
    'destination_user_uuid', # DONE
    'requested_name', # DONE
    'requested_context', # DONE
    'requested_extension', # DONE
    'requested_internal_context', # DONE
    'requested_internal_extension', # DONE
    'source_extension', # DONE
    'source_internal_context', # DONE
    'source_internal_extension', # DONE
    'source_line_id', # DONE
    'source_name', # DONE
    'source_user_uuid' # DONE
]

def float_to_clean(x):
    if np.isnan(x):
        return 'nan'
    return str(int(x))

def handle_extensions(df):
    extensions = []
    for c in df.columns:
        if "extension" in c:
            not_na_mask = ~df[c].isna()
            if df[c].dtype.name == "float64":
                df[c] = df[c].map(float_to_clean)
            elif df[c].dtype.name == "int64":
                df.loc[not_na_mask, c] = df.loc[not_na_mask, c].astype(str)
            df[c] = df[c].fillna("nan")
            extensions.append(df[c].str.lower().str.lstrip("0").values)

    extensions = pd.Series(np.unique(np.concatenate(extensions)))
    digits_counts = extensions.str.count('\d')
    digits_counts.index = extensions
    is_phone_number = (~extensions.str.contains('\*')).values & (digits_counts >= 9).values
    extension_dic_no_phones = {k: k for k in extensions[~is_phone_number]}
    extension_dic_phones = {k:  'tel-' + str(i) for i, k in enumerate(extensions[is_phone_number])}
    extension_dic = {**extension_dic_no_phones, **extension_dic_phones}
    def extension_map(x):
        # If some processing must be done with the phone numbers it might be somewhere around here
        return "extension-" + str(extension_dic[x.lower().lstrip("0")])

    for c in df.columns:
        if "extension" in c:
            df[c] = df[c].map(extension_map)
    return df

def handle_categorical(df, keyword):
    categories = []
    for c in df.columns:
        if keyword in c:
            not_na_mask = ~df[c].isna()
            if df[c].dtype.name == "float64":
                df[c] = df[c].map(float_to_clean)
            elif df[c].dtype.name == "int64":
                df.loc[not_na_mask, c] = df.loc[not_na_mask, c].astype(str)
            df[c] = df[c].fillna("nan")
            df[c] = df[c].str.lower().str.strip().str.replace(" ", "-")
            categories.append(df[c].str.lower().values)
    categories = np.unique(np.concatenate(categories))
    categories_dic= {k: i for i, k in enumerate(categories)}

    def categories_map(x):
        return keyword + "-" + str(categories_dic[x.lower()])
        
    for c in df.columns:
        if keyword in c:
            df[c] = df[c].map(categories_map)

    return df

def anonymize_cdr(df):
    df = df[columns]
    df = handle_extensions(df)
    df = handle_categorical(df, "context")
    df = handle_categorical(df, "line")
    df = handle_categorical(df, "uuid")
    df = handle_categorical(df, "name")
    return df

def read_client_dir(path):
    data = []
    for f in os.listdir(path):
        if not f.startswith('.'):
            data.append(pd.read_csv(os.path.join(path, f)))
    data = pd.concat(data, axis=0)
    return data

stack_wise_cat = [
    'id', # DONE
    'destination_extension', # DONE
    'destination_internal_context', # DONE
    'destination_internal_extension', # DONE
    'destination_line_id', # DONE
    'destination_name', # DONE
    'destination_user_uuid', # DONE
    'requested_name', # DONE
    'requested_context', # DONE
    'requested_extension', # DONE
    'requested_internal_context', # DONE
    'requested_internal_extension', # DONE
    'source_extension', # DONE
    'source_internal_context', # DONE
    'source_internal_extension', # DONE
    'source_line_id', # DONE
    'source_name', # DONE
    'source_user_uuid' # DONE
]

def merge(dfs):
    for i, df in enumerate(dfs):
        df["stack"] = i
        for c in stack_wise_cat:
            df[c] = 'stack' + str(i) + '-' + df[c].astype(str)
    df = pd.concat(dfs, axis=0, ignore_index=True).sort_values("start").reset_index(drop=True)
    return df

def make_anonymous_dataset(source_dir = "./sources/"):
    dfs = []
    for f in os.listdir(source_dir):
        if not f.startswith("_") and not f.startswith("."):
            df = read_client_dir(os.path.join(source_dir, f))
            dfs.append(anonymize_cdr(df))
    df = merge(dfs)
    df.to_csv("cdr.csv", index=False)

if __name__ == "__main__":
    make_anonymous_dataset()