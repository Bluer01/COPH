import click
from src.config import load_config
from src.file_parser import parse_file
from src.document_factory import DocumentFactory
from src.sample_processor import upload_samples, print_samples
from src.db_utils import setup_database
from src.ontology_utils import setup_ontology

@click.command()
@click.argument("filepath", type=click.Path(exists=True))
@click.option("-u", "--username", prompt="Monitoring device user's name", help="Name of monitoring device user")
@click.option("-d", "--device", prompt="Device name", help="Name of monitoring device")
@click.option("-s", "--sample_period", prompt="Interval of sample period", help="The interval a sample period represents.")
@click.option("-m", "--max_samples", prompt="Maximum samples per document", help="Most samples to upload per MongoDB document.", default=1500)
@click.option("-db", "--database", help="Database to upload to.", default='COPH')
@click.option("-c", "--collection", help="Collection to upload to.", default='measurements')
def main(filepath: str, username: str, device: str, sample_period: str,
         max_samples: int, database: str, collection: str):
    """
    Main function to process and upload data.

    :param filepath: Path to the input file
    :param username: Name of the monitoring device user
    :param device: Name of the monitoring device
    :param sample_period: Interval of the sample period
    :param max_samples: Maximum number of samples per document
    :param database: Name of the database to upload to
    :param collection: Name of the collection to upload to
    """
    config = load_config()
    config.update({
        'USERNAME': username,
        'DEVICE': device,
        'SAMPLE_PERIOD': sample_period,
        'MAX_SAMPLES': max_samples,
        'DATABASE': database,
        'COLLECTION_NAME': collection
    })

    onto = setup_ontology(config)
    db, collection = setup_database(config)
    
    data = parse_file(filepath)
    document_factory = DocumentFactory(config)

    try:
        if config['DEBUG_MODE']:
            print_samples(data=data[:100], document_factory=document_factory, config=config)
            mappings = document_factory.create_mappings(config['DEVICE'], record_data=data,
                                        database=db, ontology=onto)
            document_factory.print_mappings(mappings)
        else:
            upload_samples(data=data, document_factory=document_factory, config=config)
            mappings = document_factory.create_mappings(config['DEVICE'], record_data=data,
                                        database=db, ontology=onto)
            upload_mappings(mappings, config)
    finally:
        db.client.close()

if __name__ == "__main__":
    main()