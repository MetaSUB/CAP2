
import click
import logging
from .pangea_file_source import PangeaFileSource
from .pangea_file_uploader import PangeaFileUploader, PangeaFileAlreadyExistsError
from ..table_builder import CAPTableBuilder
from .utils import get_pangea_group

from pangea_api.contrib.tagging import Tag
from pangea_api import (
    Knex,
    User,
)
from ..table_builder.modules import (
    Kraken2TaxaTableModule,
    Kraken2CovidFastDetectModule,
    FastKraken2TableModule,
    BasicStatsTableModule,
)

logger = logging.getLogger(__name__)  # Same name as calling module


def process_module(module, module_counts, file_source, file_uploader, strict=False):
    try:
        module_count = module_counts.get(module.source_result_name, 0)
        up_to_date = file_uploader.result_is_up_to_date(module.result_name, module_count)
        if module_count > 0 and not up_to_date:
            read_counts, n_samples = module.build_result(file_source)
            file_uploader.upload_pandas(
                read_counts, module.result_name, module.field_name, n_samples=n_samples
            )
            logger.info(f'made {module.result_name} for group {file_uploader.grp.name}')
        elif up_to_date:
            logger.info(f'{module.result_name} already exists for group {file_uploader.grp.name}')
        else:
            logger.info(f'source modules not available for {module.result_name} for group {file_uploader.grp.name}')
    except:
        logger.error(f'failed to make {module.result_name} for group {file_uploader.grp.name}')
        if strict:
            raise


def process_group(pangea_group, strict=False):
    module_counts = pangea_group.get_module_counts()
    file_source, file_uploader = PangeaFileSource(pangea_group), PangeaFileUploader(pangea_group)
    modules = [
        Kraken2TaxaTableModule,
        Kraken2CovidFastDetectModule,
        FastKraken2TableModule,
        BasicStatsTableModule,
    ]
    for module in modules:
        process_module(module, module_counts, file_source, file_uploader, strict=strict)
