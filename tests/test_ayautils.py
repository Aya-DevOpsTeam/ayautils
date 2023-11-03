import ayautils
import json

with open("tests\\resources\\sample.json", "r") as src:
    obj = json.loads(s=src.read())
print(obj)
user_dm = ayautils.DocumentManager(
    primary_key="id",
    primary_document=ayautils.CsvDocument(
        path="tests\\output",
        name="users",
    ),
)
for user in obj:
    ayautils.unnest_to_csv(
        docman=user_dm,
        subj=user,
    )
user_dm.PRIMARY_DOCUMENT.write_to_file()
for doc in user_dm.SUB_DOCUMENTS:
    doc.write_to_file()
