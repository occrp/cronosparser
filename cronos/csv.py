import csv
import os

from .parser import parse


def prepare_out_folder(path):
    os.makedirs(path, exist_ok=True)


def make_csv_file_path(meta, table, out_folder):
    return os.path.join(
        out_folder,
        f"{meta['BankName']} - {table['abbr']} - {table['name']}.csv",
    )


def parse_to_csv(db_folder, out_folder):
    metadata, tables = parse(db_folder)
    prepare_out_folder(out_folder)

    for table in tables:
        # TODO: do we want to export the "FL" table?
        if table['abbr'] == 'FL' and table['name'] == 'Files':
            continue

        csv_file_path = make_csv_file_path(metadata, table, out_folder)
        with open(csv_file_path, 'w') as fh:
            writer = csv.writer(fh)
            writer.writerow([
                col['name']
                for col in table['columns']
            ])

            writer.writerows(table['records'])
