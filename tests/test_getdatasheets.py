import pandas as pd

import GetDataSheets as gds


def test_safe_filename_strips_windows_chars():
    name = 'ACME/1234:Test*Part?"<>|'
    assert gds.safe_filename(name) == "ACME_1234_Test_Part"


def test_build_output_filename_includes_brand_and_code():
    filename = gds.build_output_filename("Acme", "XK-200")
    assert filename == "XK-200 Acme.pdf"


def test_build_queries_includes_variants():
    queries = gds.build_queries("Acme", "XK-200")
    assert queries[0].endswith("datasheet filetype:pdf")
    assert "datasheet pdf" in queries[1]


def test_resolve_column_accepts_variations():
    df = pd.DataFrame(columns=["Product Code", "Brand Name"])
    brand = gds.resolve_column(df, {"brand", "brandname"})
    code = gds.resolve_column(df, {"productcode", "productid", "productnumber"})
    assert brand == "Brand Name"
    assert code == "Product Code"
