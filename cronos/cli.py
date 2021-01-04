import click

from .csv import parse_to_csv


@click.command()
@click.argument('database_dir')
@click.argument('target_dir')
def main(database_dir, target_dir):
    """Generate CSV files from a CronosPro/CronosPlus database."""
    try:
        parse_to_csv(database_dir, target_dir)
    except Exception as e:
        raise click.ClickException(e)
