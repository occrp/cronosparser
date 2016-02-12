import os
import click

from cronos.parser import CronosException
from cronos.parser import parse


@click.command()
@click.argument('database_dir')
@click.argument('target_dir')
def main(database_dir, target_dir):
    """Generate CSV files from a CronosPro/CronosPlus database."""
    if not os.path.isdir(database_dir):
        raise click.ClickException("Database directory does not exist!")
    try:
        os.makedirs(target_dir)
    except:
        pass
    try:
        parse(database_dir, target_dir)
    except CronosException as ex:
        raise click.ClickException(ex.message)
