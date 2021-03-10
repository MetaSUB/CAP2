
import click
from pangea_api import *

descriptions = {
    "megagenome::v1::mash": "compact sketches to quickly compare samples",

    "metagenscope::reads_classified": "input for the metagenscope visualization engine",
    "metagenscope::reads_classified": "input for the metagenscope visualization engine",
    "metagenscope::ave_genome_size": "input for the metagenscope visualization engine",
    "metagenscope::taxa_tree": "input for the metagenscope visualization engine",
    "metagenscope::alpha_diversity ": "input for the metagenscope visualization engine",
    "metagenscope::top_taxa": "input for the metagenscope visualization engine",
    "metagenscope::sample_similarity": "input for the metagenscope visualization engine",
    "metagenscope::volcano": "input for the metagenscope visualization engine",
    "metagenscope::covid_fast_detect": "input for the metagenscope visualization engine",

    "Other Metadata": "additional metadata tables",
    "Data Packet": "packet with a number of table summarizing results for this group",

    "cap2::metaspades": "genome and contig assembly",
    "cap2::bracken_kraken2": "species level abundance of microbes",
    "cap2::mash": "compact sketches to quickly compare samples",
    "cap2::remove_human": "reads with human genetic sequences removed",
    "cap2::jellyfish": "k-mer abundance profiles",
    "cap2::hmp_comparison": "comparison with human microbiome project samples",
    "cap2::diamond": "alignment of reads to UniRef90",
    "cap2::fastqc": "basic quality control metrics",
    "cap2::kraken2": "read level taxonomic assignments",
    "cap2::clean_reads": "error corrected and human-free reads",
    "cap2::basic_sample_stats": "GC fraction, similarity to control samples, fraction human",
    "cap2::fast_kraken2": "quick initial taxonomic profiles",

    "cap2::experimental::covid19_fast_detect": "quickly detect SARS-CoV-2 and other viruses",
    "cap2::experimental::call_covid_variants": "identify variants in SARS-CoV-2 genome relative to the reference",
    "cap2::experimental::make_covid_pileup": "pileup reads that align to the SARS-CoV-2 genome",
    "cap2::experimental::covid_genome_coverage": "calculate coverage of the SARS-CoV-2 genome",
    "cap2::experimental::make_covid_consensus_seq": "generate a consensus SARS-CoV-2 genome with all major variants in the sample",
    "cap2::experimental::align_to_covid_genome": "align reads to the SARS-CoV-2 genome",

    "cap2::capalyzer::kraken2_taxa": "table of read level taxonomic assignments for samples in this group",
    "cap2::capalyzer::basic_stats_table": "GC fraction, similarity to control samples, fraction human for all samples",
    "cap2::capalyzer::fast_kraken2": "quick initial taxonomic profiles for all samples",
    "cap2::capalyzer::kraken2_covid_fast_detect": "quickly detect SARS-CoV-2 and other viruses for all samples",
    "cap2::capalyzer::fast_kraken2_taxa": "quick initial taxonomic profilesfor all samples",

    "cap1::read_classification_proportions": "relative abundance of domain level taxa",
    "cap1::microbe_census": "estimate of average genome size",
    "cap1::krakenhll_taxonomy_profiling": "read level taonomic assignments",
    "cap1::humann2_functional_profiling": "metabolic pathway abundance",
    "cap1::bracken_abundance_estimation": "species level abundance of microbes",
    "cap1::metaphlan2_taxonomy_profiling": "species level abundance of microbes",
    "cap1::filter_human_dna": "reads with human genetic sequences removed",
    "cap1::hmp_site_dists": "comparison with human microbiome project samples",
    "cap1::adapter_removal": "reads with adapter sequences removed",

    "raw::raw_reads": "standard paired-end short reads",
    "raw::raw_single_ended_reads": "standard single-end short reads",
}


@click.group()
def main():
    pass


@main.command('group')
@click.option('-e', '--email')
@click.option('-p', '--password')
@click.argument('org_name')
@click.argument('grp_name')
def group(email, password, org_name, grp_name):
    knex = Knex(endpoint_url='https://pangeabio.io')
    User(knex, email, password).login()

    org = Organization(knex, org_name).get()
    grp = org.sample_group(grp_name).get()
    for ar in grp.get_analysis_results():
        try:
            desc = descriptions[ar.module_name]
        except KeyError:
            continue
        ar.description = desc
        ar.save()
        click.echo(f'added description to {ar}', err=True)


@main.command('samples')
@click.option('-e', '--email')
@click.option('-p', '--password')
@click.argument('org_name')
@click.argument('grp_name')
def samples(email, password, org_name, grp_name):
    knex = Knex(endpoint_url='https://pangeabio.io')
    User(knex, email, password).login()

    org = Organization(knex, org_name).get()
    grp = org.sample_group(grp_name).get()
    for sample in grp.get_samples():
        for ar in sample.get_analysis_results(cache=False):
            try:
                desc = descriptions[ar.module_name]
            except KeyError:
                continue
            ar.description = desc
            ar.save()
            click.echo(f'added description to {ar}', err=True)


if __name__ == '__main__':
    main()
