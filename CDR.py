import pandas as pd

from core.Variable import Variable, Category, Binary
from core.DataPipeline import DataPipeline


class CDR(DataPipeline):
    def __init__(self):
        super(CDR, self).__init__([
            Category("call_direction"),
            Category("destination_extension"),
            Category("destination_internal_context"),
            Category("destination_internal_extension"),
            Category("destination_line_id"),
            Category("destination_name"),
            Category("destination_user_uuid"),
            Category("requested_name"),
            Category("requested_context"),
            Category("requested_extension"),
            Category("requested_internal_context"),
            Category("requested_internal_extension"),
            Category("source_extension"),
            Category("source_internal_context"),
            Category("source_internal_extension"),
            Category("source_line_id"),
            Category("source_name"),
            Category("source_user_uuid"),
            Category("stack")
        ], [
            Binary("answered")
        ])
    
    def read_raw_data(self):
        return pd.read_csv("./cdr.csv")

if __name__ == "__main__":
    cdr = CDR()
    X, y = cdr.make_dataset()

    print(X.dtypes)
    print(y.dtypes)