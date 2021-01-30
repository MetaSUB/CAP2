import pandas as pd


CONTROL_SAMPLES_TAXONOMIC_PROFILES = pd.DataFrame.from_dict({
    'ZymoBIOMICS Microbial Community Standard': {
        'Listeria monocytogenes':   0.12,
        'Pseudomonas aeruginosa':   0.12,
        'Bacillus subtilis':        0.12,
        'Escherichia coli':         0.12,
        'Salmonella enterica':      0.12,
        'Lactobacillus fermentum':  0.12,
        'Enterococcus faecalis':    0.12,
        'Staphylococcus aureus':    0.12,
        'Saccharomyces cerevisiae': 0.02,
        'Cryptococcus neoformans':  0.02,
    },
    'ZymoBIOMICS Gut Microbiome Standard': {
        'Faecalibacterium prausnitzii': 0.14,
        'Veillonella rogosae':          0.14,
        'Roseburia hominis':            0.14,
        'Bacteroides fragilis':         0.14,
        'Prevotella corporis':          0.06,
        'Bifidobacterium adolescentis': 0.06,
        'Fusobacterium nucleatum':      0.06,
        'Lactobacillus fermentum':      0.06,
        'Clostridioides difficile':     0.015,
        'Akkermansia muciniphila':      0.015,
        'Methanobrevibacter smithii':   0.0001,
        'Salmonella enterica':          0.0001,
        'Enterococcus faecalis':        0.00001,
        'Clostridium perfringens':      0.000001,
        'Escherichia coli (JM109)':     0.028,
        'Escherichia coli (B-3008)':    0.028,
        'Escherichia coli (B-2207)':    0.028,
        'Escherichia coli (B-766)':     0.028,
        'Escherichia coli (B-1109)':    0.028,
        'Candida albicans':             0.015,
        'Saccharomyces cerevisiae':     0.014,
    },
    'ZymoBIOMICS Spike-in Control I (High Microbial Load)': {
        'Imtechella halotolerans':   0.5,
        'Allobacillus halotolerans': 0.5,
    },
    'ZymoBIOMICS Microbial Community Standard II (Log Distribution)': {
        'Listeria monocytogenes':  0.891,
        'Pseudomonas aeruginosa':  0.089,
        'Bacillus subtilis':       0.0089,
        'Saccharomyces cerevisiae':0.0089,
        'Escherichia coli':        0.00089,
        'Salmonella enterica':     0.00089,
        'Lactobacillus fermentum': 0.000089,
        'Enterococcus faecalis':   0.0000089,
        'Cryptococcus neoformans': 0.0000089,
        'Staphylococcus aureus':   0.00000089,
    },
    'ZymoBIOMICS HMW DNA Standard': {
        'Pseudomonas aeruginosa':   0.14,
        'Escherichia coli':         0.14,
        'Salmonella enterica':      0.14,
        'Enterococcus faecalis':    0.14,
        'Staphylococcus aureus':    0.14,
        'Listeria monocytogenes':   0.14,
        'Bacillus subtilis':        0.14,
        'Saccharomyces cerevisiae': 0.02,
    },
    'ZymoBIOMICS Spike-in Control II (Low Microbial Load)': {
        'Truepera radiovictrix':     0.33333,
        'Imtechella halotolerans':   0.33333,
        'Allobacillus halotolerans': 0.33333,
    },
    'Mycobiome Genomic DNA Mix (ATCC MSA-1010)': {
        'Aspergillus fumigatus':        0.1,
        'Cryptococcus neoformans':      0.1,
        'Trichophyton interdigitale':   0.1,
        'Penicillium chrysogenum':      0.1,
        'Fusarium keratoplasticum':     0.1,
        'Candida albicans':             0.1,
        'Candida glabrata':             0.1,
        'Malassezia globose':           0.1,
        'Saccharomyces cerevisiae':     0.1,
        'Cutaneotrichosporon dermatis': 0.1,
    },
    'Oral Microbiome Whole Cell Mix (ATCC MSA-2004)': {
        'Schaalia odontolytica':        0.167,
        'Prevotella melaninogenica':    0.167,
        'Fusobacterium nucleatum':      0.167,
        'Streptococcus mitis':          0.167,
        'Veillonella parvula':          0.167,
        'Haemophilus parainfluenzae':   0.167,
    },
    'Skin Microbiome Whole Cell Mix (ATCC MSA-2005)': {
        'Acinetobacter johnsonii':      0.167,
        'Corynebacterium striatum':     0.167,
        'Micrococcus luteus':           0.167,
        'Cutibacterium acnes':          0.167,
        'Staphylococcus epidermidis':   0.167,
        'Streptococcus mitis':          0.167,
    },
    'Gut Microbiome Whole cell Mix (ATCC MSA-2006)': {
        'Bacteriodes fragilis':         0.083,
        'Bacteroides vulgatus':         0.083,
        'Bifidobacterium adolescentis': 0.083,
        'Clostridioides difficile':     0.083,
        'Enterococcus faecalis':        0.083,
        'Lactobacillus plantarum':      0.083,
        'Enterobacter cloacae':         0.083,
        'Escherichia coli':             0.083,
        'Helicobacter pylori':          0.083,
        'Salmonella enterica':          0.083,
        'Yersinia enterocolitica':      0.083,
        'Fusobacterium nucleatum':      0.083,
    },
    'Vaginal Microbiome Whole Cell Mix (ATCC MSA-2007)': {
        'Gardnerella vaginalis':    0.167,
        'Lactobacillus gasseri':    0.167,
        'Mycoplasma hominis':       0.167,
        'Prevotella bivia':         0.167,
        'Streptococcus agalactiae': 0.167,
        'Lactobacillus jensenii':   0.167,
    },
    'Virome Virus Mix (ATCC MSA-2008)': {
        'Human mastadenovirus':              0.167,
        'Human herpesvirus':                 0.167,
        'Human respiratory syncytial virus': 0.167,
        'Influenza B virus':                 0.167,
        'Reovirus 3':                        0.167,
        'Zika virus':                        0.167,
    },
    '3 Strain Tagged Whole Cell Even Mix (ATCC MSA-2014)': {
        'Escherichia coli':         0.33,
        'Clostridium perfringens':  0.33,
        'Staphylococcus aureus':    0.33,
    },
    'ABRF-MGRG 6 Strain Even Mix Genomic Material (ATCC MSA-3000)': {
        'Chromobacter violaceum':     0.1667,
        'Escherichia coli':           0.1667,
        'Haloferax volcanii':         0.1667,
        'Micrococcus luteus':         0.1667,
        'Pseudomonas fluorescens':    0.1667,
        'Staphylococcus epidermidis': 0.1667,
    },
    'Metagenomic Control Material for Pathogen Detection (ATCC MSA-4000)': {
        'Acinetobacter baumannii':  0.001,
        'Enterococcus faecalis':    0.007,
        'Escherichia coli':         0.014,
        'Klebsiella pneumoniae':    0.144,
        'Neisseria meningitidis':   0.289,
        'Pseudomonas aeruginosa':   0.003,
        'Staphylococcus aureus':    0.151,
        'Streptococcus agalacitae': 0.029,
        'Streptococcus pneumoniae': 0.0289,
        'Streptococcus pyogenes':   0.072,
    },
}, orient='index')