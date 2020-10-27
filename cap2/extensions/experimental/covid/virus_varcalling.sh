## [1] run kraken2 to define reads
function kraken2_pe_micro {
  input_fastq_R1=$1
  input_fastq_R2=$2
  output_prefix=$3
  if [ -f ${output_prefix}_micro.kraken2_report ]; then
    return 1
  fi
  kraken2 --db /home/cem2009/Projects/SARS-CoV-2/database/humanmicroorg --threads 20 --output ${output_prefix}_micro.kraken2_output --report ${output_prefix}_micro.kraken2_report --report-zero-counts --paired $input_fastq_R1 $input_fastq_R2
}
export -f kraken2_pe_micro
export datafolder=/path/to/datafolder
ls ${datafolder}/*gz | rev | cut -d'/' -f1 | rev | sed -r 's/_S.+//' | uniq | \
  env_parallel -j 2 -v --lb "kraken2_pe_micro ${datafolder}/{}*R1*.gz ${datafolder}/{}*R2*.gz {}_humanmicro"
​
## [2] gather data and run kraken2fastq
for i in *output; do \
  sample=$(echo $i | sed -r "s/_humanmicro.+//"); 
  echo ${datafolder}/${sample}*R1*.gz | tr '\n' '@'; 
  echo $PWD/$i; 
done > inputsAndOutputs.txt
nsamples=$(wc -l inputsAndOutputs.txt)
sbatch --array 1-$nsamples run_kraken2fqs.slurm inputsAndOutputs.txt  # on curie; requires slurm script
​
## [3] align to wuhan ref with bwa
function bwa_to_wuhan {
  ref_path=/home/cem2009/Projects/SARS-CoV-2/reference/bwa_covid_2020.03.13_noPolyA  
  R1=$1
  R2=${R1/_R1/_R2}
  sample=$(echo $R1 | sed -r 's/_filtered.+//')
  readgroup=@RG\\tID:${sample}\\tSM:${sample}\\tPL:ILLUMINA
  bwa mem -t 4 -R $readgroup $ref_path $R1 $R2 | samtools view -b - | samtools sort -o ${sample}.bam -
}
export -f bwa_to_wuhan
ls ${datafolder}/*sars*R1*.fastq.gz | env_parallel -j 8 -v --lb "bwa_to_wuhan {}"
  
## [4] get genome coverage
for i in *.RG.bam; do bedtools genomecov -d -ibam $i > ${i/.RG.bam/.genomecov}; done
for i in *.genomecov; do sample=$(echo $i | cut -d'.' -f1); cat $i | sed -e "s/NC_045512.2/${sample}/g"; done | sed -e "1s/^/sample,base,cov/" | tr '\t' ',' > genomecov_ALL.csv
# get plot order (for R)
for i in *genomecov; do echo $i | trt; cat $i | awk '{sum+=$3} END { print sum/NR}'; done | sort -k2,2nr | head -40
​
## [5] compile outputs
echo 'sample,total,unclassified,bacteria,archaea,fungi,human,viruses,sars_cov2,genomecov_any,genomecov_10x' > reads_coverage.csv
for i in *.RG.bam; do
  sample=$(echo $(basename $i) | sed -e "s/.RG.bam//"); echo $sample | tr '\n' ','; \
  sample_kraken=/athena/masonlab/scratch/projects/metagenomics/metamed/analysis/kraken2/kraken2outputs/${sample}_humanmicro_micro.kraken2_report; \
  reads_classified=$(grep 'root$' $sample_kraken | cut -f2); \
  reads_unclassified=$(grep -P '\tunclassified' $sample_kraken | cut -f2); \
  reads_total=$(( $reads_classified + $reads_unclassified )); \
  reads_bacteria=$(grep -P '\tD\t' $sample_kraken | grep Bacteria | cut -f2); \
  reads_archaea=$(grep -P '\tD\t' $sample_kraken | grep Archaea | cut -f2); \
  reads_fungi=$(grep -P '\tK\t' $sample_kraken | grep Fungi | cut -f2); \
  reads_human=$(grep 'Homo sapiens' $sample_kraken | cut -f2); \
  reads_viruses=$(grep -P '\tD\t' $sample_kraken | grep Viruses | cut -f2); \
  reads_SARSCoV2=$(grep 'Severe acute respiratory syndrome coronavirus 2' $sample_kraken | cut -f2); \
  sample_genomecov=/athena/masonlab/scratch/projects/metagenomics/metamed/analysis/bwa/${sample}.genomecov; \
  covany=$(cat $sample_genomecov | awk '$3 > 0' | wc -l); \
  cov10x=$(cat $sample_genomecov | awk '$3 > 9' | wc -l); \
  echo $reads_total,$reads_unclassified,$reads_bacteria,$reads_archaea,$reads_fungi,$reads_human,$reads_viruses,$reads_SARSCoV2,$covany,$cov10x;
done >> reads_coverage.csv
​
## [6] call variants on bwamem BAMs (with haploid setting)
function virus_VCF {
  export SENTIEON_LICENSE=abclic01.med.cornell.edu:8990 
  export SENTIEON_INSTALL_DIR=/home/jfoox/programs/sentieon-genomics-201808.01/bin/sentieon
  SENPATH=/home/jfoox/programs/sentieon-genomics-201808.01/bin
  ref_path=/home/cem2009/Projects/SARS-CoV-2/reference/GCF_009858895.2_ASM985889v3_genomic_noPolyAtail.fna
  bam=$1
  sample=$(echo $(basename $bam) | sed -e "s/_bwamem.bam//")
  if [ -s $bam ]; then
    echo "[`date`] Indexing BAM  $sample"
    samtools index ${sample}.RG.bam
    echo "[`date`] Calling vars  $sample"
    ${SENPATH}/sentieon driver -t 4 -r $ref_path -i ${sample}.RG.bam --algo Haplotyper --ploidy 1 ${sample}.vcf  2> /dev/null
    echo "[`date`] Finished with $sample"
  else
    echo "[`date`] Empty BAM for $sample"
  fi
}
export -f virus_VCF
ls *_bwamem.dedup.bam | env_parallel -j 8 -v --lb "virus_VCF {}"
  
## [7] second pass: capture all variants and force call at those loci
parallel -j 40 "bgzip {}; tabix {}.gz" ::: *vcf
bcftools merge --threads 8 *.vcf.gz > for_2ndpass.vcf
/home/jfoox/programs/sentieon-genomics-201808.01/bin/sentieon util vcfindex for_2ndpass.vcf
function virus_VCF_secondpass {
  export SENTIEON_LICENSE=abclic01.med.cornell.edu:8990 
  export SENTIEON_INSTALL_DIR=/home/jfoox/programs/sentieon-genomics-201808.01/bin/sentieon
  SENPATH=/home/jfoox/programs/sentieon-genomics-201808.01/bin
  ref_path=/home/cem2009/Projects/SARS-CoV-2/reference/GCF_009858895.2_ASM985889v3_genomic_noPolyAtail.fna
  bam=$1
  sample=$(echo $bam | sed -e "s/_bwamem.dedup.bam.RG.bam//")
  echo "[`date`] Force calling $sample"
  ${SENPATH}/sentieon driver -t 4 -r $ref_path -i $bam --algo Haplotyper --ploidy 1 --emit_mode confident --given for_2ndpass.vcf ${sample}.2ndpass.vcf  2> /dev/null
  echo "[`date`] Finished with $sample"
}
export -f virus_VCF_secondpass
ls *_bwamem.dedup.bam.RG.bam | env_parallel -j 8 -v --lb "virus_VCF_secondpass {}"
parallel -j 40 "bgzip {}; tabix {}.gz" ::: *vcf
bcftools merge --threads 8 *.2ndpass.vcf.gz > for_matrix_ALL.vcf
​
## [8] create TSV matrix of samples and global variants
bcftools query -f '%CHROM\t%POS\t%REF\t%ALT\t[%DP|%GT\t]\n' for_matrix_ALL.vcf  > for_matrix_ALL.ADandGT
header=$(grep -m1 "bcftools_mergeCommand" for_matrix_ALL.vcf \
  | sed -r "s/.+--threads 8 //" | tr ' ' '@' \
  | sed -e "s/.2ndpass.vcf.gz//g" | sed -e "s/^/Chr@Pos@Ref@Alt@/" | sed -r "s/;@.+//")
cat for_matrix_ALL.ADandGT | sed -e "1s/^/$header\n/" | tr '@' '\t' > for_matrix_ALL.ADandGT2
mv for_matrix_ALL.ADandGT2 for_matrix_ALL.ADandGT
pct90=$(printf %.0f $(bc <<< "scale=1; $(echo $header | tr '@' '\n' | wc -l) * 0.9"))
run_convert_ADandGT_to_matrix.py for_matrix_ALL.ADandGT $pct90 $pct90
/home/jfoox/transpose_tabs.sh for_matrix_ALL.ADandGT.toMatrix | tr ' ' '\t' | sed -e "s/Chr_Pos_Ref_Alt/Sample/" > for_matrix_ALL.ADandGT.toMatrix.t.tsv
